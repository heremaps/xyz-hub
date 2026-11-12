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

import static com.here.xyz.util.Random.randomAlpha;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.OLD_LAYOUT;
import static com.here.xyz.util.db.pg.LazySpaceTables.LevelState.EXISTS;
import static com.here.xyz.util.db.pg.LazySpaceTables.LevelState.NEVER_MATERIALIZED;
import static com.here.xyz.util.db.pg.LazySpaceTables.MaterializationResult.CREATED;
import static com.here.xyz.util.db.pg.LazySpaceTables.MaterializationResult.EXISTED;
import static com.here.xyz.util.db.pg.LazySpaceTables.Profile.BARE;
import static com.here.xyz.util.db.pg.LazySpaceTables.Profile.FULL;
import static com.here.xyz.util.db.pg.LazySpaceTables.TEMPLATE_TABLE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.test.sql.base.SQLITBase;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;
import com.here.xyz.util.db.pg.IndexHelper.SystemIndex;
import com.here.xyz.util.db.pg.LazySpaceTables.Level;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelCheck;
import com.here.xyz.util.db.pg.LazySpaceTables.MaterializationResult;
import com.here.xyz.util.db.pg.LazySpaceTables.TemplateProbe;
import com.here.xyz.util.db.pg.MaterializationTimeoutException.Phase;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("lazy-space-tables")
public class LazySpaceTablesIT extends SQLITBase {
  private static final Set<String> SCHEMAS = ConcurrentHashMap.newKeySet();
  //A schema with the template, installed once for the tests that need it
  private static final String TEMPLATE_SCHEMA = "lazy_" + randomAlpha(5);
  private static final long BUDGET_MILLIS = 20_000;
  private static DatabaseSettings settings;
  private static PooledDataSources dsp;

  @BeforeAll
  public static void setUp() throws Exception {
    SCHEMAS.add(TEMPLATE_SCHEMA);
    settings = new DatabaseSettings(LazySpaceTablesIT.class.getSimpleName() + "_" + randomAlpha(5))
        .withApplicationName(LazySpaceTablesIT.class.getSimpleName())
        .withHost(PG_HOST)
        .withDb(PG_DB)
        .withUser(PG_USER)
        .withPassword(PG_PW)
        .withSchema(TEMPLATE_SCHEMA)
        .withDbMaxPoolSize(12)
        .withScriptResourcePaths(List.of(new ScriptResourcePath("/sql", "hub", "common")));
    dsp = new PooledDataSources(settings);
    assertNull(install(TEMPLATE_SCHEMA));
  }

  @AfterAll
  public static void dropSchemas() throws Exception {
    try {
      for (String schema : SCHEMAS)
        new SQLQuery("DROP SCHEMA IF EXISTS ${schema} CASCADE").withVariable("schema", schema).write(dsp);
    }
    finally {
      dsp.close();
    }
  }

  private static String newSchema() throws SQLException {
    String schema = "lazy_" + randomAlpha(5);
    SCHEMAS.add(schema);
    new SQLQuery("CREATE SCHEMA ${schema}").withVariable("schema", schema).write(dsp);
    return schema;
  }

  //All names of a comparison get the same length, so that generated names are truncated alike
  private static String newTable(String prefix) {
    return prefix + randomAlpha(TEMPLATE_TABLE.length() - prefix.length());
  }

  private static TemplateProbe install(String schema) throws SQLException {
    try (Connection connection = dsp.getWriter().getConnection()) {
      return LazySpaceTables.installTemplate(connection, schema, settings.getSearchPath(), BUDGET_MILLIS);
    }
  }

  private static MaterializationResult materialize(String schema, String table) throws SQLException {
    return LazySpaceTables.materialize(dsp, schema, table, table, FULL, List.of(), null, BUDGET_MILLIS);
  }

  private static void createEager(String schema, String table) throws SQLException {
    SQLQuery.batchOf(XyzSpaceTableHelper.buildCreateSpaceTableQueries(schema, table, List.of(), table, OLD_LAYOUT)).writeBatch(dsp);
  }

  private static void execute(String sql, String schema) throws SQLException {
    new SQLQuery(sql).withVariable("schema", schema).write(dsp);
  }

  private static boolean exists(String schema, String relation) throws SQLException {
    return new SQLQuery("SELECT to_regclass(#{name}) IS NOT NULL")
        .withNamedParameter("name", "\"" + schema + "\".\"" + relation + "\"")
        .run(dsp, rs -> rs.next() && rs.getBoolean(1));
  }

  private static TemplateProbe probe(String schema) throws SQLException {
    return LazySpaceTables.buildTemplateProbe(schema).run(dsp, rs -> rs.next() ? LazySpaceTables.readTemplateProbe(rs) : null);
  }

  /**
   * @return One row per relation (name and kind) and one per index (name, table and definition) of the table, its partitions and
   *  sequences, with the table name replaced by "&lt;t&gt;"
   */
  private static Set<String> catalogRows(String schema, String table) throws SQLException {
    return new SQLQuery("""
        SELECT replace(c.relname, #{table}, '<t>') || ':' || c.relkind::TEXT AS row FROM pg_class c
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = #{schema} AND starts_with(c.relname, #{table})
        UNION ALL
        SELECT 'index:' || replace(ic.relname, #{table}, '<t>') || ':' || replace(c.relname, #{table}, '<t>') || ':'
            || replace(replace(pg_get_indexdef(i.indexrelid), #{table}, '<t>'), #{schema}, '<s>') FROM pg_index i
          JOIN pg_class ic ON ic.oid = i.indexrelid
          JOIN pg_class c ON c.oid = i.indrelid
          JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE n.nspname = #{schema} AND starts_with(c.relname, #{table})
        """)
        .withNamedParameter("schema", schema)
        .withNamedParameter("table", table)
        .run(dsp, rs -> {
          Set<String> rows = new TreeSet<>();
          while (rs.next())
            rows.add(rs.getString("row"));
          return rows;
        });
  }

  @Test
  public void templateSetEqualsAnEagerCreate() throws Exception {
    String schema = newSchema();
    assertNull(install(schema), "The installation must create the template");
    String eagerTable = newTable("eager_");
    createEager(schema, eagerTable);

    Set<String> templateRows = catalogRows(schema, TEMPLATE_TABLE);
    assertFalse(templateRows.isEmpty());
    assertEquals(catalogRows(schema, eagerTable), templateRows);
    assertFalse(exists(schema, TEMPLATE_TABLE + "_branches_seq"), "The builder creates no branches sequence any more");
    assertTrue(probe(schema).isIdentical());
  }

  @Test
  public void versionSequenceOfTheTemplateIsExhausted() throws Exception {
    String schema = newSchema();
    //A placeholder left in the batch would fail the installation with 42601
    assertNull(install(schema));

    assertEquals("0 1 1", new SQLQuery("""
        SELECT s.seqmin || ' ' || s.seqmax || ' ' || q.last_value FROM pg_sequence s
          JOIN pg_sequences q ON q.schemaname = #{schema} AND q.sequencename = #{sequence}
          WHERE s.seqrelid = to_regclass(#{qualified})
        """)
        .withNamedParameter("schema", schema)
        .withNamedParameter("sequence", TEMPLATE_TABLE + "_version_seq")
        .withNamedParameter("qualified", "\"" + schema + "\".\"" + TEMPLATE_TABLE + "_version_seq\"")
        .run(dsp, rs -> rs.next() ? rs.getString(1) : null));

    try (Connection connection = dsp.getWriter().getConnection(); Statement stmt = connection.createStatement()) {
      stmt.execute("SELECT nextval('\"" + schema + "\".\"" + TEMPLATE_TABLE + "_version_seq\"')");
      fail("The sequence must be exhausted");
    }
    catch (SQLException e) {
      assertEquals("2200H", e.getSQLState());
      assertTrue(LazySpaceTables.isTemplateWriteException(e), e.getMessage());
    }

    //An exhausted sequence of a real space is not a write to the template
    String table = newTable("t_");
    createEager(schema, table);
    String sequence = "\"" + schema + "\".\"" + table + "_version_seq\"";
    try (Connection connection = dsp.getWriter().getConnection(); Statement stmt = connection.createStatement()) {
      stmt.execute("ALTER SEQUENCE " + sequence + " MINVALUE 0 MAXVALUE 1 RESTART WITH 1");
      stmt.execute("SELECT nextval('" + sequence + "')");
      stmt.execute("SELECT nextval('" + sequence + "')");
      fail("The sequence must be exhausted");
    }
    catch (SQLException e) {
      assertEquals("2200H", e.getSQLState());
      assertFalse(LazySpaceTables.isTemplateWriteException(e), e.getMessage());
    }
  }

  @Test
  public void guardRejectsEveryWrite() throws Exception {
    for (String table : List.of(TEMPLATE_TABLE, TEMPLATE_TABLE + "_head", TEMPLATE_TABLE + "_p0")) {
      String t = "\"" + TEMPLATE_SCHEMA + "\".\"" + table + "\"";
      for (String statement : List.of(
          "INSERT INTO " + t + " (id, version, operation) VALUES ('a', 1, 'I')",
          "INSERT INTO " + t + " (id, version, operation) VALUES ('a', 1, 'I') ON CONFLICT DO NOTHING",
          "UPDATE " + t + " SET author = 'x'",
          "DELETE FROM " + t,
          "TRUNCATE " + t,
          "MERGE INTO " + t + " t USING (SELECT 'a' AS id) s ON t.id = s.id "
              + "WHEN NOT MATCHED THEN INSERT (id, version, operation) VALUES (s.id, 1, 'I')"))
        try (Connection connection = dsp.getWriter().getConnection(); Statement stmt = connection.createStatement()) {
          stmt.execute(statement);
          fail("The guard must reject: " + statement);
        }
        catch (SQLException e) {
          assertEquals(SQLError.EMPTY_TEMPLATE_WRITE.errorCode, e.getSQLState(), statement);
          assertTrue(LazySpaceTables.isTemplateWriteException(e), statement);
        }
    }
  }

  @Test
  public void featureWriterCannotWriteToTheTemplate() throws Exception {
    Modification modification = new Modification()
        .withFeatureData(new FeatureCollection().withFeatures(List.of(new Feature().withId("f1")
            .withGeometry(new Point().withCoordinates(new PointCoordinates(8, 50, 0)))
            .withProperties(new Properties()))))
        .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY);
    Map<String, Object> queryContext = new FeatureWriterQueryContextBuilder()
        .withSchema(TEMPLATE_SCHEMA)
        .withTables(List.of(TEMPLATE_TABLE))
        .withSpaceContext(SpaceContext.DEFAULT)
        .withHistoryEnabled(false)
        .build();
    SQLQuery writeQuery = new SQLQuery("SELECT write_features(#{modifications}, 'Modifications', #{author}, #{responseDataExpected})")
        .withNamedParameter("modifications", XyzSerializable.serialize(Set.of(modification)))
        .withNamedParameter("author", "someAuthor")
        .withNamedParameter("responseDataExpected", true)
        .withContext(queryContext);

    SQLException e = assertThrows(SQLException.class, () -> SQLQuery.batchOf(writeQuery).writeBatch(dsp));
    //The feature writer takes a version before it writes, so the exhausted version sequence rejects the write before the guard does
    assertEquals("2200H", LazySpaceTables.sqlStateOf(e.getNextException() != null ? e.getNextException() : e), e.toString());
    assertTrue(e.toString().contains(TEMPLATE_TABLE + "_version_seq"), e.toString());
    //The exhausted template sequence counts as a write to the template
    assertTrue(LazySpaceTables.isTemplateWriteError(e), e.toString());
    assertEquals(0, (int) new SQLQuery("SELECT count(*) FROM ${schema}.${table}")
        .withVariable("schema", TEMPLATE_SCHEMA)
        .withVariable("table", TEMPLATE_TABLE)
        .run(dsp, rs -> rs.next() ? rs.getInt(1) : -1));
  }

  @Test
  public void installationEndsWithinItsBudget() throws Exception {
    String schema = newSchema();
    List<SQLQuery> statements = new ArrayList<>(List.of(new SQLQuery("CREATE TABLE ${schema}.${table} (id INT)")
        .withVariable("schema", schema)
        .withVariable("table", TEMPLATE_TABLE)));
    for (int i = 0; i < 5; i++)
      statements.add(new SQLQuery("SELECT pg_sleep(0.9)"));

    try (Connection connection = dsp.getWriter().getConnection()) {
      long start = System.currentTimeMillis();
      SQLException e = assertThrows(SQLException.class,
          () -> LazySpaceTables.installTemplate(connection, schema, settings.getSearchPath(), 2_000, statements));
      long duration = System.currentTimeMillis() - start;

      assertEquals(MaterializationTimeoutException.QUERY_CANCELED, LazySpaceTables.sqlStateOf(e));
      assertTrue(duration >= 1_500 && duration < 4_000, "The installation must end after its budget of 2 s, took " + duration + " ms");
      assertTrue(connection.getAutoCommit(), "The connection must be back in autocommit");
    }
    assertFalse(exists(schema, TEMPLATE_TABLE), "The installation must be rolled back");
  }

  @Test
  public void probeWithoutARowFailsTheInstallation() throws Exception {
    String schema = newSchema();
    SQLQuery emptyProbe = new SQLQuery("SELECT 1 WHERE false");

    try (Connection connection = dsp.getWriter().getConnection()) {
      SQLException e = assertThrows(SQLException.class, () -> LazySpaceTables.installTemplate(connection, schema,
          settings.getSearchPath(), BUDGET_MILLIS, LazySpaceTables.buildTemplateQueries(schema), emptyProbe));
      assertTrue(e.getMessage().contains("no row"), e.getMessage());
      assertTrue(connection.getAutoCommit(), "The connection must be back in autocommit");
    }
    assertFalse(exists(schema, TEMPLATE_TABLE), "Nothing may be installed without a probe row");
    assertTrue(probe(schema).isAbsent());
  }

  @Test
  public void checkLevelsReportsTheTemplateAndRequireTemplateOnlyThrowsForSubstitutedLevels() throws Exception {
    String schema = newSchema();
    String existing = newTable("t_");
    String missing = newTable("t_");
    createEager(schema, existing);
    List<Level> levels = List.of(new Level(0, "space0", existing), new Level(1, "space1", missing));

    LevelCheck withoutTemplate = LazySpaceTables.checkLevels(dsp, schema, levels);
    assertEquals(List.of(EXISTS, NEVER_MATERIALIZED), withoutTemplate.states());
    assertFalse(withoutTemplate.templatePresent());
    //A write target that the caller materializes needs no template
    LazySpaceTables.requireTemplate(schema, withoutTemplate, Set.of());
    TemplateUnavailableException e = assertThrows(TemplateUnavailableException.class,
        () -> LazySpaceTables.requireTemplate(schema, withoutTemplate, Set.of(1)));
    assertEquals("XX000", e.getSQLState());
    assertTrue(e.getMessage().startsWith("Empty-space template missing"));

    assertNull(install(schema));
    LevelCheck withTemplate = LazySpaceTables.checkLevels(dsp, schema, levels);
    assertEquals(List.of(EXISTS, NEVER_MATERIALIZED), withTemplate.states());
    assertTrue(withTemplate.templatePresent());
    LazySpaceTables.requireTemplate(schema, withTemplate, Set.of(1));
  }

  @Test
  public void checkLevelsReportsATableDroppedAfterItsMaterializationAsNeverMaterialized() throws Exception {
    String table = newTable("t_");
    List<Level> levels = List.of(new Level(0, table, table));
    assertEquals(List.of(NEVER_MATERIALIZED), LazySpaceTables.checkLevels(dsp, TEMPLATE_SCHEMA, levels).states());

    assertEquals(CREATED, materialize(TEMPLATE_SCHEMA, table));
    assertEquals(List.of(EXISTS), LazySpaceTables.checkLevels(dsp, TEMPLATE_SCHEMA, levels).states());

    execute("DROP TABLE ${schema}." + table + " CASCADE", TEMPLATE_SCHEMA);
    assertEquals(List.of(NEVER_MATERIALIZED), LazySpaceTables.checkLevels(dsp, TEMPLATE_SCHEMA, levels).states());
  }

  @Test
  public void fullAndBareProfilesEqualAnEagerCreate() throws Exception {
    String schema = TEMPLATE_SCHEMA;
    String full = newTable("full_");
    String eagerFull = newTable("eagf_");
    assertEquals(CREATED, LazySpaceTables.materialize(dsp, schema, full, full, FULL, List.of("foo", "it's"), null, BUDGET_MILLIS));
    SQLQuery.batchOf(XyzSpaceTableHelper.buildCreateSpaceTableQueries(schema, eagerFull,
        List.of(new OnDemandIndex().withPropertyPath("foo")), eagerFull, OLD_LAYOUT)).writeBatch(dsp);
    assertEquals(catalogRows(schema, eagerFull), catalogRows(schema, full), "FULL must create what an eager create creates");
    assertEquals(EXISTED, LazySpaceTables.materialize(dsp, schema, full, full, FULL, List.of("foo"), null, BUDGET_MILLIS));

    List<SystemIndex> whitelist = List.of(SystemIndex.VERSION_ID, SystemIndex.NEXT_VERSION, SystemIndex.OPERATION);
    String bare = newTable("bare_");
    String eagerBare = newTable("eagb_");
    assertEquals(CREATED, LazySpaceTables.materialize(dsp, schema, bare, bare, BARE, List.of("foo"), whitelist, BUDGET_MILLIS));
    SQLQuery.batchOf(XyzSpaceTableHelper.buildCreateSpaceTableQueries(schema, eagerBare, List.of(), eagerBare, OLD_LAYOUT, whitelist))
        .writeBatch(dsp);
    assertEquals(catalogRows(schema, eagerBare), catalogRows(schema, bare), "BARE must create only the whitelisted indexes");
  }

  @Test
  public void parallelMaterializationsCreateTheTableOnce() throws Exception {
    String table = newTable("t_");
    int threads = 8;
    CountDownLatch start = new CountDownLatch(1);
    ExecutorService executor = Executors.newFixedThreadPool(threads);
    try {
      List<Future<MaterializationResult>> results = new ArrayList<>();
      for (int i = 0; i < threads; i++)
        results.add(executor.submit(() -> {
          start.await();
          return materialize(TEMPLATE_SCHEMA, table);
        }));
      start.countDown();

      List<MaterializationResult> outcomes = new ArrayList<>();
      for (Future<MaterializationResult> result : results)
        outcomes.add(result.get(60, TimeUnit.SECONDS));
      assertEquals(1, outcomes.stream().filter(outcome -> outcome == CREATED).count(), "Results: " + outcomes);
      assertEquals(threads - 1, outcomes.stream().filter(outcome -> outcome == EXISTED).count(), "Results: " + outcomes);
    }
    finally {
      executor.shutdownNow();
    }
    assertTrue(exists(TEMPLATE_SCHEMA, table));
  }

  @Test
  public void materializationWaitsForADeletionAndCreatesTheTableAfterIt() throws Exception {
    String table = newTable("t_");
    assertEquals(CREATED, materialize(TEMPLATE_SCHEMA, table));
    Set<String> fullSet = catalogRows(TEMPLATE_SCHEMA, table);

    ExecutorService executor = Executors.newSingleThreadExecutor();
    try (Connection lockConnection = dsp.getWriter().getConnection()) {
      //A space deletion holds the session lock on the table while it drops the table
      LockHelper.advisoryLock(table, lockConnection);
      execute("DROP TABLE ${schema}." + table + " CASCADE", TEMPLATE_SCHEMA);
      long start = System.currentTimeMillis();
      Future<MaterializationResult> result = executor.submit(() -> materialize(TEMPLATE_SCHEMA, table));
      Thread.sleep(1_000);
      assertFalse(result.isDone(), "The materialization must wait for the lock of the deletion");
      LockHelper.advisoryUnlock(table, lockConnection);

      assertEquals(CREATED, result.get(30, TimeUnit.SECONDS));
      assertTrue(System.currentTimeMillis() - start >= 1_000);
    }
    finally {
      executor.shutdownNow();
    }
    assertEquals(fullSet, catalogRows(TEMPLATE_SCHEMA, table));
  }

  @Test
  public void heldLockLeadsToALockTimeout() throws Exception {
    String table = newTable("t_");
    try (Connection lockConnection = dsp.getWriter().getConnection()) {
      LockHelper.advisoryLock(table, lockConnection);
      long start = System.currentTimeMillis();
      MaterializationTimeoutException e = assertThrows(MaterializationTimeoutException.class,
          () -> LazySpaceTables.materialize(dsp, TEMPLATE_SCHEMA, table, table, FULL, List.of(), null, 3_000));
      long duration = System.currentTimeMillis() - start;
      LockHelper.advisoryUnlock(table, lockConnection);

      assertEquals(MaterializationTimeoutException.LOCK_NOT_AVAILABLE, e.getSQLState());
      assertEquals(Phase.LOCK, e.getPhase());
      assertTrue(duration >= 2_500 && duration < 6_000, "The lock wait must end after the budget of 3 s, took " + duration + " ms");
    }
    assertFalse(exists(TEMPLATE_SCHEMA, table));
  }

  @Test
  public void tooSmallBudgetAndReservedNamesAreRejected() throws Exception {
    String table = newTable("t_");
    MaterializationTimeoutException e = assertThrows(MaterializationTimeoutException.class,
        () -> LazySpaceTables.materialize(dsp, TEMPLATE_SCHEMA, table, table, FULL, List.of(), null, 2_999));
    assertEquals(MaterializationTimeoutException.QUERY_CANCELED, e.getSQLState());
    assertEquals(Phase.BUDGET, e.getPhase());
    assertFalse(exists(TEMPLATE_SCHEMA, table));

    assertThrows(ReservedTableNameException.class, () -> materialize(TEMPLATE_SCHEMA, TEMPLATE_TABLE));
  }

  @Test
  public void conflictWithAnExistingTypeFailsAndLeavesNoRelation() throws Exception {
    String table = newTable("t_");
    execute("CREATE TYPE ${schema}." + table + " AS ENUM ('a')", TEMPLATE_SCHEMA);

    SQLException e = assertThrows(SQLException.class, () -> materialize(TEMPLATE_SCHEMA, table));
    assertFalse(e instanceof MaterializationTimeoutException);
    assertEquals("42710", LazySpaceTables.sqlStateOf(e));
    assertEquals(Set.of(), catalogRows(TEMPLATE_SCHEMA, table), "The failed materialization must leave no relation");
  }
}
