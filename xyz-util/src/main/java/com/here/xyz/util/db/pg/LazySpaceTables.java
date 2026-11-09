/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.util.db.pg;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.OLD_LAYOUT;
import static com.here.xyz.util.db.pg.LockHelper.buildAdvisoryXactLockQuery;
import static com.here.xyz.util.db.pg.MaterializationTimeoutException.LOCK_NOT_AVAILABLE;
import static com.here.xyz.util.db.pg.MaterializationTimeoutException.QUERY_CANCELED;
import static com.here.xyz.util.db.pg.SQLError.EMPTY_TEMPLATE_WRITE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.HEAD_TABLE_SUFFIX;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueries;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.sequenceName;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import com.here.xyz.util.db.pg.MaterializationTimeoutException.Phase;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Helpers for lazy spaces: spaces whose tables are created by their first write. Until then, reads of such a space
 * use the empty-space template, a set of empty tables per connector schema that rejects every write.
 * Shared by the connector and the job steps.
 */
public class LazySpaceTables {
  /**
   * The name of the empty-space template's root table. The version allows a future template with other columns.
   */
  public static final String TEMPLATE_TABLE = "xyz_empty_space_v1";
  /**
   * No space table may have a name with this prefix.
   */
  public static final String RESERVED_PREFIX = "xyz_empty_space_";
  /**
   * The space ID in the table comment of the template.
   */
  public static final String TEMPLATE_SPACE_ID = "xyz-empty-space-template";
  /**
   * The event parameter with the on-demand indexes to create when a write materializes the table.
   */
  public static final String ON_DEMAND_INDEXES = "onDemandIndexes";
  /**
   * The message of the error that the guard triggers of the template raise, with SQLSTATE
   * {@link SQLError#EMPTY_TEMPLATE_WRITE}.
   */
  public static final String TEMPLATE_WRITE_MESSAGE = "Internal error: write to the empty-space template rejected";
  /**
   * The guard triggers of the template: one on the root, one on the head table and one on the first partition.
   */
  public static final int GUARD_TRIGGER_COUNT = 3;
  /**
   * The name of the statement-level triggers on the template that reject every write.
   */
  public static final String GUARD_TRIGGER = "xyz_empty_space_guard";
  /**
   * A materialization with less budget than this is not started.
   */
  public static final long MIN_MATERIALIZATION_BUDGET_MILLIS = 3_000;
  private static final Logger logger = LogManager.getLogger(LazySpaceTables.class);
  private static final long LOCK_TIMEOUT_MARGIN_MILLIS = 200;
  private static final String TEMPLATE_WRITE_MESSAGE_PART = "empty-space template rejected";
  //sequence_generator_limit_exceeded, raised by nextval() on an exhausted sequence
  private static final String SEQUENCE_EXHAUSTED = "2200H";
  //A sequence of the template as the quoted name in the message of nextval(), also with escaped quotes (no space table can
  //have a name with the reserved prefix)
  private static final Pattern TEMPLATE_SEQUENCE_REFERENCE = Pattern.compile("\"" + TEMPLATE_TABLE + "_[A-Za-z0-9_]*seq\\\\?\"");
  private static final String GUARD_FUNCTION = TEMPLATE_TABLE + "_guard";
  private static final String FIRST_PARTITION_SUFFIX = "_p0";
  private static final String EXTENDS = "extends";
  private static final String SPACE_ID = "spaceId";
  private static final int MAX_DEPTH = 2;

  private LazySpaceTables() {}

  /**
   * One level of a space's composition.
   *
   * @param depth 0 for the space itself, 1 for the space it extends, 2 for the space that one extends
   * @param spaceId The space ID if the level's params carry it (the extended levels do), otherwise null
   * @param table The level's physical table name, or null for a level without an explicit or with an empty table name
   */
  public record Level(int depth, String spaceId, String table) {}

  /**
   * The state of the table of a level that carries the lazyTable flag. A level whose flag was cleared is not lazy and
   * has no state.
   */
  public enum LevelState {
    /**
     * The table exists.
     */
    EXISTS,
    /**
     * No table on a level that still carries lazyTable: never written, or lost while its flag was not cleared yet;
     * the level reads as the template and the next write materializes it.
     */
    NEVER_MATERIALIZED
  }

  /**
   * The result of one existence check.
   *
   * @param states One state per checked level, in the order of the checked levels
   * @param templatePresent Whether the root table of the template exists
   */
  public record LevelCheck(List<LevelState> states, boolean templatePresent) {}

  /**
   * What the catalog says about the relations of the template.
   *
   * @param anyRelation Whether any relation with a template name exists
   * @param rootExists Whether the template's root table exists
   * @param comment The raw comment of the root table, or null
   * @param guardTriggers The number of enabled guard triggers
   * @param sequencesExhausted Whether the version sequence of the template is exhausted
   */
  public record TemplateProbe(boolean anyRelation, boolean rootExists, String comment, int guardTriggers,
      boolean sequencesExhausted) {

    /**
     * @return Whether no relation of the template exists, so it can be installed
     */
    public boolean isAbsent() {
      return !anyRelation;
    }

    /**
     * @return Whether the template is complete and was created by the installer
     */
    public boolean isIdentical() {
      return rootExists && guardTriggers == GUARD_TRIGGER_COUNT && sequencesExhausted
          && TEMPLATE_SPACE_ID.equals(commentSpaceId(comment));
    }

    /**
     * @return The parts in which the relations differ from an identical template, empty if it is identical
     */
    public List<String> differences() {
      List<String> differences = new ArrayList<>();
      if (!rootExists)
        differences.add("root table missing");
      else if (!TEMPLATE_SPACE_ID.equals(commentSpaceId(comment)))
        differences.add("comment " + comment);
      if (guardTriggers != GUARD_TRIGGER_COUNT)
        differences.add(guardTriggers + " of " + GUARD_TRIGGER_COUNT + " guard triggers");
      if (!sequencesExhausted)
        differences.add("version sequence not exhausted");
      return differences;
    }

    //An unparsable comment yields null and thus a template that is not identical
    private static Object commentSpaceId(String comment) {
      if (comment == null)
        return null;
      try {
        Map<?, ?> parsed = XyzSerializable.deserialize(comment, Map.class);
        return parsed == null ? null : parsed.get(SPACE_ID);
      }
      catch (JsonProcessingException | RuntimeException e) {
        return null;
      }
    }
  }

  /**
   * The set of tables and indexes that a materialization creates.
   */
  public enum Profile {
    /**
     * Everything an eager space creation creates, including the on-demand indexes.
     */
    FULL,
    /**
     * The tables with only the system indexes a bulk load keeps, for an import that creates the other indexes later.
     */
    BARE
  }

  public enum MaterializationResult {
    /**
     * The space is not lazy, nothing was checked.
     */
    NOT_LAZY,
    /**
     * The table existed already.
     */
    EXISTED,
    /**
     * The table was created by this call.
     */
    CREATED
  }

  /**
   * @param table A physical table name
   * @return Whether the name has the prefix that is reserved for the empty-space template
   */
  public static boolean isReserved(String table) {
    return table != null && table.startsWith(RESERVED_PREFIX);
  }

  /**
   * Returns the levels of the provided params that carry the lazyTable flag ({@link Space#isLazyTable(Map)}): the params
   * themselves, "extends" and "extends.extends". A level with another storage ID than the composite carries no flag.
   *
   * @param params The event params or the resolved space params
   * @return The lazy levels, ordered by depth
   */
  public static List<Level> lazyLevels(Map<String, Object> params) {
    List<Level> levels = new ArrayList<>();
    Map<String, Object> level = params;
    for (int depth = 0; depth <= MAX_DEPTH && level != null; depth++) {
      if (Space.isLazyTable(level))
        levels.add(new Level(depth, stringOrNull(level.get(SPACE_ID)), nonEmptyOrNull(level.get(TABLE_NAME))));
      level = asMap(level.get(EXTENDS));
    }
    return levels;
  }

  public static boolean hasLazyLevel(Map<String, Object> params) {
    return !lazyLevels(params).isEmpty();
  }

  /**
   * Returns a deep copy of the params, in which the table name of each provided level is the template.
   * The lazyTable flags stay, the provided params are not changed.
   *
   * @param params The event params
   * @param missingLevels The levels to read from the template
   * @return The substituted copy
   */
  public static Map<String, Object> substitute(Map<String, Object> params, Collection<Level> missingLevels) {
    Map<String, Object> copy = deepCopy(params);
    for (Level missingLevel : missingLevels) {
      Map<String, Object> level = copy;
      for (int depth = 0; depth < missingLevel.depth() && level != null; depth++)
        level = asMap(level.get(EXTENDS));
      if (level == null)
        throw new IllegalArgumentException("The params have no level of depth " + missingLevel.depth());
      level.put(TABLE_NAME, TEMPLATE_TABLE);
    }
    return copy;
  }

  /**
   * Applies the character rules of the space configuration to the on-demand indexes of an event, because they reach the
   * index creation without that validation.
   *
   * @param onDemandIndexes The property paths, may be null
   * @return The non-empty strings without ' and \, in their original order
   */
  public static List<String> validateOnDemandIndexes(List<?> onDemandIndexes) {
    if (onDemandIndexes == null)
      return List.of();
    return onDemandIndexes.stream()
        .filter(index -> index instanceof String property && !property.isEmpty() && !property.contains("'")
            && !property.contains("\\"))
        .map(index -> (String) index)
        .toList();
  }

  /**
   * Checks one exception, without walking its causes.
   *
   * @return Whether the exception was raised by a guard trigger of the template, or by the exhausted sequence of the template,
   *  which a writer that takes a version first hits before the guard
   */
  public static boolean isTemplateWriteException(SQLException e) {
    if (e == null)
      return false;
    if (EMPTY_TEMPLATE_WRITE.errorCode.equals(e.getSQLState()))
      return true;
    String message = e.getMessage();
    if (message == null)
      return false;
    return message.contains(TEMPLATE_WRITE_MESSAGE_PART)
        || SEQUENCE_EXHAUSTED.equals(e.getSQLState()) && TEMPLATE_SEQUENCE_REFERENCE.matcher(message).find();
  }

  /**
   * Walks the causes and, for each {@link SQLException}, the chain of {@link SQLException#getNextException()}, because a
   * batch error may carry the guard error only in that chain.
   *
   * @return Whether any of them was raised by a guard trigger of the template
   */
  public static boolean isTemplateWriteError(Throwable throwable) {
    Set<Throwable> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    Deque<Throwable> pending = new ArrayDeque<>();
    if (throwable != null)
      pending.push(throwable);
    while (!pending.isEmpty()) {
      Throwable current = pending.pop();
      if (!seen.add(current))
        continue;
      if (current instanceof SQLException sqlException) {
        if (isTemplateWriteException(sqlException))
          return true;
        if (sqlException.getNextException() != null)
          pending.push(sqlException.getNextException());
      }
      if (current.getCause() != null)
        pending.push(current.getCause());
    }
    return false;
  }

  /**
   * @param schema The connector schema
   * @return The key of the transaction lock that serializes the installations of the template in the schema
   */
  public static String templateLockKey(String schema) {
    return TEMPLATE_TABLE + ":" + schema;
  }

  /**
   * Builds the statements that create the empty-space template in the provided schema: the tables, indexes, comment and
   * sequences of a new space, a guard function, a guard trigger on the root, the head table and the first partition, the
   * exhaustion of the version sequence and an ANALYZE. They run in one transaction, see
   * {@link #installTemplate(Connection, String, List, long)}.
   *
   * @param schema The connector schema
   * @return The statements in the order of their execution
   */
  public static List<SQLQuery> buildTemplateQueries(String schema) {
    List<SQLQuery> queries = new ArrayList<>(buildCreateSpaceTableQueries(schema, TEMPLATE_TABLE, List.of(), TEMPLATE_SPACE_ID,
        OLD_LAYOUT));
    //OR REPLACE: dropping the template tables leaves the function in place, a re-installation must not fail on it
    queries.add(new SQLQuery("""
        CREATE OR REPLACE FUNCTION ${schema}.${function}() RETURNS trigger LANGUAGE plpgsql AS $guard$
        BEGIN
          RAISE EXCEPTION '${{message}}' USING ERRCODE = '${{errorCode}}';
        END
        $guard$""")
        .withVariable(SCHEMA, schema)
        .withVariable("function", GUARD_FUNCTION)
        .withQueryFragment("message", TEMPLATE_WRITE_MESSAGE)
        .withQueryFragment("errorCode", EMPTY_TEMPLATE_WRITE.errorCode));
    for (String table : templateGuardedTables())
      queries.add(new SQLQuery("CREATE TRIGGER ${trigger} BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON ${schema}.${table} "
          + "FOR EACH STATEMENT EXECUTE FUNCTION ${schema}.${function}()")
          .withVariable("trigger", GUARD_TRIGGER)
          .withVariable(SCHEMA, schema)
          .withVariable(TABLE, table)
          .withVariable("function", GUARD_FUNCTION));
    //After the one call, every further nextval() fails
    queries.add(new SQLQuery("ALTER SEQUENCE ${schema}.${sequence} MINVALUE 0 MAXVALUE 1 RESTART WITH 1")
        .withVariable(SCHEMA, schema)
        .withVariable("sequence", templateVersionSequence()));
    queries.add(new SQLQuery("SELECT nextval('${schema}.${sequence}')")
        .withVariable(SCHEMA, schema)
        .withVariable("sequence", templateVersionSequence()));
    queries.add(new SQLQuery("ANALYZE ${schema}.${table}")
        .withVariable(SCHEMA, schema)
        .withVariable(TABLE, TEMPLATE_TABLE));
    return queries;
  }

  private static List<String> templateGuardedTables() {
    return List.of(TEMPLATE_TABLE, TEMPLATE_TABLE + HEAD_TABLE_SUFFIX, TEMPLATE_TABLE + FIRST_PARTITION_SUFFIX);
  }

  private static String templateVersionSequence() {
    return sequenceName(TEMPLATE_TABLE, "version");
  }

  /**
   * Builds a complete SELECT that reads the state of the template's relations from the catalog, in the column shape of the
   * template row of the script snapshot: {@code kind}, the script columns as NULL, and {@code tpl_table},
   * {@code tpl_any_relation}, {@code tpl_root_exists}, {@code tpl_comment}, {@code tpl_guard_triggers} and
   * {@code tpl_sequences_exhausted}. It raises no error for any catalog content: names are looked up in pg_class, which yields
   * NULL for a missing relation, and the sequence functions are only called on a readable sequence.
   *
   * @param schema The connector schema
   * @return The probe query, see {@link #readTemplateProbe(ResultSet)}
   */
  public static SQLQuery buildTemplateProbe(String schema) {
    String table = TEMPLATE_TABLE;
    //NOTE: The names are resolved by a scan of pg_class rather than with to_regclass. to_regclass answers from the backend's
    // catalog cache, which is not refreshed while the transaction waits for an advisory lock, so the probe under the lock of the
    // installation would miss a template that a concurrent installer committed meanwhile.
    //The parameter names carry the table, so probes of several template versions can be combined in one query.
    return new SQLQuery("""
        SELECT 'template'::TEXT AS kind, NULL::TEXT AS schema_name, NULL::BOOLEAN AS has_hash, NULL::TEXT AS hash,
          NULL::BOOLEAN AS has_version, NULL::TEXT AS version, #{tpl_table}::TEXT AS tpl_table,
          (r.root IS NOT NULL OR r.head IS NOT NULL OR r.p0 IS NOT NULL OR r.seq IS NOT NULL) AS tpl_any_relation,
          r.root IS NOT NULL AS tpl_root_exists,
          CASE WHEN r.root IS NOT NULL THEN obj_description(r.root, 'pg_class') END AS tpl_comment,
          (SELECT count(*)::INT FROM pg_trigger t
            WHERE t.tgname = #{tpl_trigger} AND t.tgenabled = 'O' AND t.tgrelid IN (r.root, r.head, r.p0)) AS tpl_guard_triggers,
          coalesce((SELECT CASE WHEN c.relkind = 'S' THEN CASE WHEN has_sequence_privilege(c.oid, 'SELECT') THEN
              (SELECT s.seqmax = 1 FROM pg_sequence s WHERE s.seqrelid = c.oid) AND pg_sequence_last_value(c.oid::REGCLASS) = 1 END END
            FROM pg_class c WHERE c.oid = r.seq), false) AS tpl_sequences_exhausted
        FROM (SELECT ${{root}} AS root, ${{head}} AS head, ${{p0}} AS p0, ${{seq}} AS seq) r
        """.replace("#{tpl_", "#{" + table + "_"))
        .withQueryFragment("root", relationOid(table + "_schema", table + "_root"))
        .withQueryFragment("head", relationOid(table + "_schema", table + "_head"))
        .withQueryFragment("p0", relationOid(table + "_schema", table + "_p0"))
        .withQueryFragment("seq", relationOid(table + "_schema", table + "_seq"))
        .withNamedParameter(table + "_table", table)
        .withNamedParameter(table + "_trigger", GUARD_TRIGGER)
        .withNamedParameter(table + "_schema", schema)
        .withNamedParameter(table + "_root", table)
        .withNamedParameter(table + "_head", table + HEAD_TABLE_SUFFIX)
        .withNamedParameter(table + "_p0", table + FIRST_PARTITION_SUFFIX)
        .withNamedParameter(table + "_seq", templateVersionSequence());
  }

  /**
   * @return A scalar subquery for the OID of the relation in pg_class, NULL if there is none. It reads pg_class with the snapshot of
   *  the statement, so it sees relations that were committed while the transaction waited for a lock.
   */
  private static String relationOid(String schemaParameter, String nameParameter) {
    return "(SELECT c.oid FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = #{" + schemaParameter
        + "} AND c.relname = #{" + nameParameter + "})";
  }

  private static SQLQuery buildTableExistsQuery(String schema, String table) {
    return new SQLQuery("SELECT ${{oid}} IS NOT NULL")
        .withQueryFragment("oid", relationOid("schema", "table"))
        .withNamedParameter("schema", schema)
        .withNamedParameter("table", table);
  }

  /**
   * Reads the current row of a result set with the columns of {@link #buildTemplateProbe(String)}.
   */
  public static TemplateProbe readTemplateProbe(ResultSet rs) throws SQLException {
    return new TemplateProbe(rs.getBoolean("tpl_any_relation"), rs.getBoolean("tpl_root_exists"), rs.getString("tpl_comment"),
        rs.getInt("tpl_guard_triggers"), rs.getBoolean("tpl_sequences_exhausted"));
  }

  /**
   * Installs the empty-space template into the provided schema in one transaction on the provided connection, bounded in total
   * by the provided budget. The transaction sets its own statement, lock and idle timeouts and search path, takes the
   * transaction lock {@link #templateLockKey(String)}, probes again under the lock and creates the template only if none of its
   * relations exists by then. Any failure rolls back the whole transaction. The autocommit mode of the connection is restored
   * in every case.
   *
   * @param connection The connection of the script installer
   * @param schema The connector schema
   * @param searchPath The script schemas, which are put on the search path after the schema, "public" and "topology"
   * @param budgetMillis The total time the installation may take
   * @return <code>null</code> if this call created the template, otherwise the probe read under the lock
   * @throws SQLException If the installation failed, with SQLSTATE 57014 if the budget was used up
   */
  public static TemplateProbe installTemplate(Connection connection, String schema, List<String> searchPath, long budgetMillis)
      throws SQLException {
    return installTemplate(connection, schema, searchPath, budgetMillis, buildTemplateQueries(schema));
  }

  static TemplateProbe installTemplate(Connection connection, String schema, List<String> searchPath, long budgetMillis,
      List<SQLQuery> templateQueries) throws SQLException {
    return installTemplate(connection, schema, searchPath, budgetMillis, templateQueries, buildTemplateProbe(schema));
  }

  static TemplateProbe installTemplate(Connection connection, String schema, List<String> searchPath, long budgetMillis,
      List<SQLQuery> templateQueries, SQLQuery probeQuery) throws SQLException {
    long start = System.currentTimeMillis();
    long deadline = start + budgetMillis;
    boolean autoCommit = connection.getAutoCommit();
    try {
      checkDeadline(deadline);
      connection.setAutoCommit(false);
      try (Statement stmt = connection.createStatement()) {
        execute(stmt, deadline, "SET LOCAL statement_timeout = " + budgetMillis + "; SET LOCAL lock_timeout = "
            + lockTimeoutMillis(budgetMillis) + "; SET LOCAL idle_in_transaction_session_timeout = " + budgetMillis);
        execute(stmt, deadline, "SET LOCAL search_path = " + PooledDataSources.compileSearchPath(schema, searchPath));
        execute(stmt, deadline, buildLockQuery(templateLockKey(schema)));

        TemplateProbe probe = query(connection, probeQuery, deadline, rs -> rs.next() ? readTemplateProbe(rs) : null);
        //Fail closed: without a probe row nothing tells whether the template exists
        if (probe == null)
          throw new SQLException("The template probe returned no row.");
        if (!probe.isAbsent()) {
          //A concurrent installer won
          connection.commit();
          return probe;
        }

        //NOTE: substitute() on the batch substitutes all members without placeholders, which a plain Statement can execute
        SQLQuery.batchOf(templateQueries).substitute();
        for (SQLQuery query : templateQueries)
          stmt.addBatch(query.text());
        applyTimeout(stmt, deadline);
        stmt.executeBatch();
        checkDeadline(deadline);
        connection.commit();
      }
      return null;
    }
    catch (SQLException | RuntimeException e) {
      rollbackQuietly(connection);
      throw e;
    }
    finally {
      connection.setAutoCommit(autoCommit);
    }
  }

  /**
   * Builds one statement that checks the existence of the tables of the provided levels and of the template's root table.
   *
   * @param schema The connector schema
   * @param levels The levels to check, each with its table name
   * @return The query, to be run on the writer unless the caller accepts replica lag
   */
  public static SQLQuery buildCheckLevelsQuery(String schema, List<Level> levels) {
    List<String> columns = new ArrayList<>();
    Map<String, Object> parameters = new HashMap<>();
    for (int i = 0; i < levels.size(); i++) {
      if (levels.get(i).table() == null)
        throw new IllegalArgumentException("The level of depth " + levels.get(i).depth() + " has no table name.");
      columns.add("to_regclass(#{t" + i + "}) IS NOT NULL");
      parameters.put("t" + i, qualifiedName(schema, levels.get(i).table()));
    }
    columns.add("to_regclass(#{template}) IS NOT NULL");
    parameters.put("template", qualifiedName(schema, TEMPLATE_TABLE));
    return new SQLQuery("SELECT " + String.join(", ", columns)).withNamedParameters(parameters);
  }

  /**
   * @param levels The levels the query of {@link #buildCheckLevelsQuery(String, List)} was built for
   * @return The handler that reads its result
   */
  public static ResultSetHandler<LevelCheck> checkLevelsHandler(List<Level> levels) {
    return rs -> {
      if (!rs.next())
        throw new SQLException("The level check returned no row.");
      List<LevelState> states = new ArrayList<>();
      for (int i = 0; i < levels.size(); i++)
        states.add(rs.getBoolean(i + 1) ? LevelState.EXISTS : LevelState.NEVER_MATERIALIZED);
      return new LevelCheck(List.copyOf(states), rs.getBoolean(levels.size() + 1));
    };
  }

  /**
   * Checks the existence of the tables of the provided levels and of the template with one statement on the writer.
   *
   * @param dataSourceProvider The data source provider
   * @param schema The connector schema
   * @param levels The levels to check, each with its table name
   * @return One state per level, in the order of the levels, and whether the template's root table exists
   */
  public static LevelCheck checkLevels(DataSourceProvider dataSourceProvider, String schema, List<Level> levels) throws SQLException {
    return buildCheckLevelsQuery(schema, levels).run(dataSourceProvider, checkLevelsHandler(levels));
  }

  /**
   * Throws if levels are to be read from the template, but the template does not exist. The write target that the caller
   * materializes is not such a level.
   *
   * @param schema The connector schema, for the message
   * @param check The result of {@link #checkLevels(DataSourceProvider, String, List)}
   * @param substitutedDepths The depths of the levels without a table that the caller substitutes by the template
   * @throws TemplateUnavailableException If a level is substituted and the template is missing
   */
  public static void requireTemplate(String schema, LevelCheck check, Set<Integer> substitutedDepths)
      throws TemplateUnavailableException {
    if (!substitutedDepths.isEmpty() && !check.templatePresent())
      throw TemplateUnavailableException.missing(schema);
  }

  /**
   * Builds the statements that create the tables of a lazy space.
   *
   * @param schema The connector schema
   * @param table The table name
   * @param spaceId The space ID, for the table comment
   * @param profile FULL creates what an eager space creation creates, BARE only the provided system indexes
   * @param onDemandIndexes The property paths of the on-demand indexes, only used by FULL; invalid entries are dropped
   * @param bareSystemIndexes The system indexes to create, only used by BARE; null for none
   * @return The statements
   */
  public static List<SQLQuery> buildMaterializationQueries(String schema, String table, String spaceId, Profile profile,
      List<String> onDemandIndexes, List<SystemIndex> bareSystemIndexes) {
    if (profile == Profile.BARE)
      return buildCreateSpaceTableQueries(schema, table, List.of(), spaceId, OLD_LAYOUT,
          bareSystemIndexes == null ? List.of() : bareSystemIndexes);
    List<OnDemandIndex> indexes = validateOnDemandIndexes(onDemandIndexes).stream()
        .map(property -> new OnDemandIndex().withPropertyPath(property))
        .toList();
    return buildCreateSpaceTableQueries(schema, table, indexes, spaceId, OLD_LAYOUT);
  }

  /**
   * Creates the tables of a lazy space if they do not exist, on the writer, in one transaction under the advisory lock of the
   * table, which is also the key of the space creation and deletion. The lock wait and the DDL are bounded by the budget on the
   * server and on the client.
   *
   * @param dataSourceProvider The data source provider
   * @param schema The connector schema
   * @param table The table name
   * @param spaceId The space ID, for the table comment and the log
   * @param profile See {@link #buildMaterializationQueries(String, String, String, Profile, List, List)}
   * @param onDemandIndexes See {@link #buildMaterializationQueries(String, String, String, Profile, List, List)}
   * @param bareSystemIndexes See {@link #buildMaterializationQueries(String, String, String, Profile, List, List)}
   * @param budgetMillis The total time the materialization may take
   * @return CREATED or EXISTED
   * @throws ReservedTableNameException For a table name with the reserved prefix
   * @throws MaterializationTimeoutException If the budget is below {@link #MIN_MATERIALIZATION_BUDGET_MILLIS} or was used up
   * @throws SQLException For any other error, unchanged
   */
  public static MaterializationResult materialize(DataSourceProvider dataSourceProvider, String schema, String table, String spaceId,
      Profile profile, List<String> onDemandIndexes, List<SystemIndex> bareSystemIndexes, long budgetMillis) throws SQLException {
    if (isReserved(table))
      throw new ReservedTableNameException(table);
    List<Level> levels = List.of(new Level(0, spaceId, table));
    if (checkLevels(dataSourceProvider, schema, levels).states().get(0) == LevelState.EXISTS)
      return MaterializationResult.EXISTED;
    if (budgetMillis < MIN_MATERIALIZATION_BUDGET_MILLIS)
      throw new MaterializationTimeoutException("Too little time left to materialize table " + table + " (" + budgetMillis + " ms)",
          QUERY_CANCELED, Phase.BUDGET, null);

    long start = System.currentTimeMillis();
    long deadline = start + budgetMillis;
    List<SQLQuery> queries = buildMaterializationQueries(schema, table, spaceId, profile, onDemandIndexes, bareSystemIndexes);
    SQLException failure;
    try (Connection connection = dataSourceProvider.getWriter().getConnection()) {
      boolean autoCommit = connection.getAutoCommit();
      Phase phase = Phase.LOCK;
      try (Statement stmt = connection.createStatement()) {
        connection.setAutoCommit(false);
        execute(stmt, deadline, "SET LOCAL lock_timeout = '" + lockTimeoutMillis(budgetMillis) + "ms'; SET LOCAL statement_timeout = '"
            + budgetMillis + "ms'");
        execute(stmt, deadline, buildLockQuery(table));
        phase = Phase.DDL;
        //The re-check under the lock reads pg_class, see buildTemplateProbe() for why it does not use to_regclass
        if (query(connection, buildTableExistsQuery(schema, table), deadline, rs -> rs.next() && rs.getBoolean(1))) {
          connection.commit();
          return MaterializationResult.EXISTED;
        }
        SQLQuery.batchOf(queries).substitute();
        for (SQLQuery query : queries)
          stmt.addBatch(query.text());
        applyTimeout(stmt, deadline);
        stmt.executeBatch();
        connection.commit();
        logger.info("Materialized table {} of space {} with profile {} and {} indexes in {} ms", table, spaceId, profile,
            indexCount(profile, onDemandIndexes, bareSystemIndexes), System.currentTimeMillis() - start);
        return MaterializationResult.CREATED;
      }
      catch (SQLException e) {
        rollbackQuietly(connection);
        String sqlState = sqlStateOf(e);
        if (LOCK_NOT_AVAILABLE.equals(sqlState) || QUERY_CANCELED.equals(sqlState))
          throw new MaterializationTimeoutException("Materialization of table " + table + " timed out in phase " + phase + " after "
              + (System.currentTimeMillis() - start) + " ms", sqlState, phase, e);
        failure = e;
      }
      finally {
        connection.setAutoCommit(autoCommit);
      }
    }
    //A concurrent materialization may have created the table
    if (checkLevels(dataSourceProvider, schema, levels).states().get(0) == LevelState.EXISTS)
      return MaterializationResult.EXISTED;
    throw failure;
  }

  private static int indexCount(Profile profile, List<String> onDemandIndexes, List<SystemIndex> bareSystemIndexes) {
    if (profile == Profile.BARE)
      return bareSystemIndexes == null ? 0 : bareSystemIndexes.size();
    return SystemIndex.values().length + validateOnDemandIndexes(onDemandIndexes).size();
  }

  /**
   * @return The SQLSTATE of the exception, or of the first exception in its chain of next exceptions or causes that has one
   */
  static String sqlStateOf(SQLException e) {
    Throwable current = e;
    for (int depth = 0; current != null && depth < 10; depth++) {
      if (current instanceof SQLException sqlException) {
        if (sqlException.getSQLState() != null)
          return sqlException.getSQLState();
        if (sqlException.getNextException() != null) {
          current = sqlException.getNextException();
          continue;
        }
      }
      current = current.getCause() == current ? null : current.getCause();
    }
    return null;
  }

  /**
   * The lock timeout ends a lock wait slightly before the client-side query timeout at the end of the budget, so that a lock wait
   * is reported as such (55P03) and not as a cancellation (57014).
   */
  private static long lockTimeoutMillis(long budgetMillis) {
    return Math.max(1, budgetMillis - LOCK_TIMEOUT_MARGIN_MILLIS);
  }

  private static String buildLockQuery(String key) {
    return new SQLQuery("DO $lock$ BEGIN ${{lockQuery}} END $lock$")
        .withQueryFragment("lockQuery", buildAdvisoryXactLockQuery(key))
        .substitute()
        .text();
  }

  private static String qualifiedName(String schema, String table) {
    return "\"" + schema.replace("\"", "\"\"") + "\".\"" + table.replace("\"", "\"\"") + "\"";
  }

  private static void execute(Statement stmt, long deadline, String sql) throws SQLException {
    applyTimeout(stmt, deadline);
    stmt.execute(sql);
  }

  private static <R> R query(Connection connection, SQLQuery query, long deadline, ResultSetHandler<R> handler) throws SQLException {
    try {
      PreparedStatement stmt = query.getPreparedStatement(connection);
      applyTimeout(stmt, deadline);
      try (ResultSet rs = stmt.executeQuery()) {
        return handler.handle(rs);
      }
    }
    finally {
      query.closeStatement();
    }
  }

  /**
   * Sets the time left until the deadline as query timeout, rounded up to whole seconds.
   *
   * @throws SQLException With SQLSTATE 57014 if the deadline has passed
   */
  private static void applyTimeout(Statement stmt, long deadline) throws SQLException {
    long left = checkDeadline(deadline);
    stmt.setQueryTimeout((int) Math.min(Integer.MAX_VALUE, (left + 999) / 1_000));
  }

  private static long checkDeadline(long deadline) throws SQLException {
    long left = deadline - System.currentTimeMillis();
    if (left <= 0)
      throw new SQLException("The time budget is used up.", QUERY_CANCELED);
    return left;
  }

  private static void rollbackQuietly(Connection connection) {
    try {
      if (!connection.getAutoCommit())
        connection.rollback();
    }
    catch (SQLException e) {
      logger.warn("Rollback failed.", e);
    }
  }

  private static String stringOrNull(Object value) {
    return value instanceof String string ? string : null;
  }

  //An empty table name counts as absent, as for the query runners, which then derive the name from the space ID
  private static String nonEmptyOrNull(Object value) {
    return value instanceof String string && !string.isEmpty() ? string : null;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> asMap(Object value) {
    return value instanceof Map ? (Map<String, Object>) value : null;
  }

  @SuppressWarnings("unchecked")
  private static <T> T deepCopy(T value) {
    if (value instanceof Map<?, ?> map) {
      Map<Object, Object> copy = new LinkedHashMap<>();
      map.forEach((key, entry) -> copy.put(key, deepCopy(entry)));
      return (T) copy;
    }
    if (value instanceof List<?> list) {
      List<Object> copy = new ArrayList<>();
      list.forEach(entry -> copy.add(deepCopy(entry)));
      return (T) copy;
    }
    return value;
  }
}
