/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.psql;

import com.amazonaws.services.lambda.runtime.Context;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.connectors.StorageConnector;
import com.here.xyz.events.Event;
import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.events.QueryEvent;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.CountResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.HealthStatus;
import com.here.xyz.responses.XyzResponse;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Array;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


@SuppressWarnings("SqlDialectInspection")
public abstract class PSQLRequestStreamHandler extends StorageConnector {

  private static final Logger logger = LogManager.getLogger();

  Event event;

  private static final String PREFIX = "\\$\\{";
  private static final String SUFFIX = "\\}";
  private static final String VAR_SCHEMA = "${schema}";
  private static final String VAR_TABLE = "${table}";

  public static class CachedConfig {

    public CachedConfig(final DataSource source, final PSQLConfig config) {
      this.dataSource = source;
      this.config = config;
    }

    final PSQLConfig config;
    final DataSource dataSource;
  }

  /**
   * The configuration for the current event.
   */
  private static Map<String, PSQLConfig> configs = new HashMap<>();

  /**
   * The data source connections factory.
   */
  private static Map<String, DataSource> dataSources = new HashMap<>();

  /**
   * The data source connections factory.
   */
  private static Map<String, DataSource> replicaDataSources = new HashMap<>();

  /**
   * The write data source for the current event.
   */
  DataSource dataSource;

  /**
   * The read data source for the current event.
   */
  DataSource readDataSource;

  /**
   * The config for the current event.
   */
  PSQLConfig config;

  private static final String TIMEOUT_EXCEPTION_STRING = "canceling statement due to statement timeout";
  private static final String XYZ_CONFIG_SCHEMA = "xyz_config";
  private static final int IDX_MIN_THRESHOLD = 10000;
  protected static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()";
  protected static int ON_DEMAND_IDX_LIM = 4;

  @Override
  public XyzResponse processEvent(Event event) throws Exception {
    try {
      return super.processEvent(event);
    } catch (Exception e) {
      final String message = e.getMessage();
      if (message != null && message.contains(TIMEOUT_EXCEPTION_STRING)) {
        throw new ErrorResponseException(streamId, XyzError.TIMEOUT, "The request timed out.");
      }
      throw e;
    }
  }

  @Override
  protected synchronized void initialize(Event event) throws Exception {
    this.event = event;
    final String ecps = PSQLConfig.getECPS(event);

    if (!configs.containsKey(ecps)) {
      logger.info("{} - Create new config and data source for ECPS string: '{}'", streamId, ecps);
      final PSQLConfig config = initializeConfig(event, context);

      final ComboPooledDataSource source = getComboPooledDataSource(config.host(), config.port(), config.database(), config.user(),
          config.password(), config.applicationName(), config.maxPostgreSQLConnections());
      dataSources.put(ecps, source);

      Map<String, String> m = new HashMap<>();
      m.put(C3P0EXT_CONFIG_SCHEMA, config.schema());
      source.setExtensions(m);

      if (config.replica() != null) {
        final ComboPooledDataSource replicaDataSource = getComboPooledDataSource(config.replica(), config.port(), config.database(),
            config.user(), config.password(), config.applicationName(), config.maxPostgreSQLConnections());
        replicaDataSources.put(ecps, replicaDataSource);

        replicaDataSource.setExtensions(m);
      }

      configs.put(ecps, config);
    }

    // Set the for write operations
    dataSource = dataSources.get(ecps);
    // Set the data source for read operations.
    readDataSource = (replicaDataSources.containsKey(ecps) && (event.getPreferPrimaryDataSource() == null
        || event.getPreferPrimaryDataSource() == Boolean.FALSE)) ? replicaDataSources.get(ecps) : dataSource;

    config = configs.get(ecps);
    logger.info("{} - Connect to database: jdbc:postgresql://{}:{}/{}?user={}&password=***  |ecps={}", streamId, config.host(),
        config.port(), config.database(), config.user(), ecps);
  }

  private ComboPooledDataSource getComboPooledDataSource(String host, int port, String database, String user,
      String password, String applicationName, int maxPostgreSQLConnections) {
    final ComboPooledDataSource cpds = new ComboPooledDataSource();

    cpds.setJdbcUrl(
        String.format("jdbc:postgresql://%1$s:%2$d/%3$s?ApplicationName=%4$s&tcpKeepAlive=true", host, port, database, applicationName));

    cpds.setUser(user);
    cpds.setPassword(password);

    cpds.setInitialPoolSize(1);
    cpds.setMinPoolSize(1);
    cpds.setAcquireIncrement(1);
    cpds.setMaxPoolSize(maxPostgreSQLConnections);

    cpds.setConnectionCustomizerClassName(PSQLXyzConnector.XyzConnectionCustomizer.class.getName());

    return cpds;
  }

  /**
   * Returns the configuration bound to the given context.
   */
  public abstract PSQLConfig initializeConfig(Event event, Context context) throws Exception;

  private synchronized void setup() {
    final int xyz_ext_version = 122;
    boolean functionsUpToDate = false;
    boolean hasPropertySearch = (event.getConnectorParams() != null && event.getConnectorParams().get("propertySearch") == Boolean.TRUE);

    boolean autoIndexing = (event.getConnectorParams() != null && event.getConnectorParams().get("autoIndexing") == Boolean.TRUE);
    ON_DEMAND_IDX_LIM = (event.getConnectorParams() != null && event.getConnectorParams().get("onDemandIdxLimit") != null) ?
        (Integer) event.getConnectorParams().get("onDemandIdxLimit") : ON_DEMAND_IDX_LIM;

    final String checkExtentions = "SELECT COALESCE(array_agg(extname) @> '{postgis,postgis_topology,tsm_system_rows"
        + (hasPropertySearch ? ",dblink" : "") + "}', false) as all_ext_av,"
        + "COALESCE(array_agg(extname)) as ext_av, "
        + "(select current_setting('is_superuser')) as is_su "
        + "FROM (\n"
        + "	SELECT extname FROM pg_extension \n"
        + "		WHERE extname in('postgis','postgis_topology','tsm_system_rows','dblink')"
        + "	order by extname\n"
        + ") A";

    final String checkSchemasAndIdxTable = "SELECT array_agg(nspname) @> ARRAY['" + config.schema() + "'] as main_schema, "
        + " array_agg(nspname) @> ARRAY['xyz_config'] as config_schema, "
        + "(SELECT (to_regclass('" + XYZ_CONFIG_SCHEMA + ".xyz_idxs_status') IS NOT NULL) as idx_table) "
        + "FROM( "
        + "	SELECT nspname::text FROM pg_catalog.pg_namespace "
        + "		WHERE nspowner <> 1 "
        + ")a ";

    final String searchEnsureVersionFnct = "SELECT routine_name FROM information_schema.routines "
        + "WHERE routine_type='FUNCTION' AND specific_schema='" + config.schema()
        + "' AND routine_name='xyz_ext_version';";

    final String xyzFunctionCheck = "select " + config.schema() + ".xyz_ext_version()";

    try (final Connection connection = dataSource.getConnection()) {

      /** Check if database is prepared to work with PSQL Connector. Therefore its needed to check Extensions, Schemas, Tables and Functions.*/
      Statement stmt = connection.createStatement();
      ResultSet rs = stmt.executeQuery(checkExtentions);

      if (rs.next()) {
        if (!rs.getBoolean("all_ext_av")) {
          /** Create Missing IDX_Maintenance Table */
          if (rs.getString("is_su").equalsIgnoreCase("on")) {
            stmt.execute("CREATE EXTENSION IF NOT EXISTS postgis SCHEMA public;");
            stmt.execute("CREATE EXTENSION IF NOT EXISTS postgis_topology");
            stmt.execute("CREATE EXTENSION IF NOT EXISTS tsm_system_rows SCHEMA public");
            if (hasPropertySearch) {
              stmt.execute("CREATE EXTENSION IF NOT EXISTS dblink SCHEMA public;");
            }
          } else {
            logger.error("{} - Not allowed to create missing extentions on database': {}@{}. Currently installed are: {}",
                streamId, config.user(), config.database(), rs.getString("ext_av"));
            /** Cannot proceed without extensions!
             * postgis,postgis_topology -> provides all GIS functions which are essential!
             * tsm_system_rows -> Is used for generating statistics
             * dblink -> Is used for Auto+On-Demand Indexing
             * Without */
            return;
          }
        }
      }

      stmt = connection.createStatement();
      /** Find Missing Schemas and check if IDX_Maintenance Table is available */
      rs = stmt.executeQuery(checkSchemasAndIdxTable);

      if (rs.next()) {
        final boolean mainSchema = rs.getBoolean("main_schema");
        boolean configSchema = rs.getBoolean("config_schema");
        final boolean idx_table = rs.getBoolean("idx_table");

        try {
          /** Create Missing Schemas */
          if (!mainSchema) {
            logger.info("{} - Create missing Schema {} on {}!", streamId, config.schema(), config.database());
            stmt.execute("CREATE SCHEMA IF NOT EXISTS \"" + config.schema() + "\";");
          }

          if (!configSchema && hasPropertySearch) {
            logger.info("{} - Create missing Schema {} on {}!", streamId, XYZ_CONFIG_SCHEMA, config.database());
            stmt.execute("CREATE SCHEMA IF NOT EXISTS \"" + XYZ_CONFIG_SCHEMA + "\";");
            stmt.execute("CREATE TABLE IF NOT EXISTS  " + XYZ_CONFIG_SCHEMA + ".xyz_storage (id VARCHAR(50) primary key, config JSONB);");
            stmt.execute("CREATE TABLE IF NOT EXISTS  " + XYZ_CONFIG_SCHEMA
                + ".xyz_space (id VARCHAR(50) primary key, owner VARCHAR (50), cid VARCHAR (50), config JSONB);");
          }

          if (!idx_table && hasPropertySearch) {
            /** Create Missing IDX_Maintenance Table */
            stmt.execute("CREATE TABLE IF NOT EXISTS " + XYZ_CONFIG_SCHEMA + ".xyz_idxs_status "
                + "( "
                + "  runts timestamp with time zone, "
                + "  spaceid text NOT NULL, "
                + "  schem text, "
                + "  idx_available jsonb,"
                + "  idx_proposals jsonb, "
                + "  idx_creation_finished boolean, "
                + "  count bigint, "
                + "  prop_stat jsonb, "
                + "  idx_manual jsonb, "
                + "  CONSTRAINT xyz_idxs_status_pkey PRIMARY KEY (spaceid) "
                + "); ");
            stmt.execute("INSERT INTO xyz_config.xyz_idxs_status (spaceid,count) VALUES ('idx_in_progress','0') "
                + " ON CONFLICT DO NOTHING; ");
          }
        } catch (Exception e) {
          logger.warn("{} - Failed to create missing Schema(s) on database': {} {}", streamId, config.database(), e);
        }
      }

      stmt.execute("SET search_path=" + config.schema() + ",h3,public,topology;");

      stmt = connection.createStatement();
      rs = stmt.executeQuery(searchEnsureVersionFnct);
      if (rs.next()) {
        /** If xyz_ext_version exists, use it to evaluate if the functions needs to get updated*/
        rs = stmt.executeQuery(xyzFunctionCheck);
        if (rs.next()) {
          functionsUpToDate = (rs.getInt(1) == xyz_ext_version);
        }
      }

      if (!functionsUpToDate) {
        /** Need to apply the PSQL-script! */
        String content = readResource("/xyz_ext.sql");

        stmt = connection.createStatement();
        stmt.execute("SET search_path=" + config.schema() + ",h3,public,topology;");
        stmt.execute(content);

        logger.info("{} - Successfully created missing SQL-Functions on database {}!", streamId, config.database());
      } else {
        logger.info("{} - All required SQL-Functions are already present on database {}!", streamId, config.database());
      }
    } catch (Exception e) {
      logger.error("{} - Failed to create missing SQL-Functions on database {} : {}", streamId, config.database(), e);
    }

    if (hasPropertySearch) {
      /** Trigger Auto-Indexing and or On-Demand Index Maintenance  */
      try (final Connection connection = dataSource.getConnection()) {
        final String writeStatistics = "SELECT xyz_write_newest_statistics('" + config.schema()
            + "',ARRAY['wikvaya','" + config.user() + "']," + IDX_MIN_THRESHOLD + ");";
        final String analyseStatistic = "SELECT xyz_write_newest_idx_analyses('" + config.schema() + "')";
        final String createIDX = "SELECT * from xyz_create_idxs_over_dblink('" + config.schema() + "',100, 0,'" + config.user()
            + "','" + config.password() + "','" + config.database() + "'," + config.port() + ",'" + config.schema()
            + ",h3,public,topology)')";

        final String checkRunningQueries = "SELECT * FROM xyz_index_status();";

        Statement stmt = connection.createStatement();
        ResultSet rs = stmt.executeQuery(checkRunningQueries);

        int progress = 0;

        if (rs.next()) {
          /**
           * ((progress  &  (1<<0)) == (1<<0) = statisticsInProgress
           * ((progress  &  (1<<1)) == (1<<1) = analyseInProgress
           * ((progress  &  (1<<2)) == (1<<2) = idxCreationInProgress
           * ((progress  &  (1<<3)) == (1<<3) = idx_mode=16 (disable indexing completely)
           * ((progress  &  (1<<4)) == (1<<4) = idx_mode=32 (disable auto-indexer)
           */
          progress = rs.getInt(1);
        }

        if (progress == 0 || progress == 32) {
          /** no process is running */
          stmt.execute("SET statement_timeout = " + (15 * 60 * 1000) + " ;");
          stmt.execute(writeStatistics);

          if (progress == 0 && autoIndexing) {
            /** Pay attention - Lambda timeout is currently set to 25s */
            stmt.execute(analyseStatistic);
          }

          /** CREATE INDICES */
          stmt.execute(createIDX);
          stmt.execute("SET statement_timeout = " + PSQLXyzConnector.STATEMENT_TIMEOUT_SECONDS * 1000 + " ;");
        }
      } catch (Exception e) {
        logger.error("{} - Failed run auto-indexing on database {} : {}", streamId, config.database(), e);
      }
    }

    if (true) // check h3 availability 1. test if version function exists, 2. test if version is outdated compared with H3CoreVersion
    {
      try (final Connection connection = dataSource.getConnection();
          final Statement stmt = connection.createStatement();) {
        final int H3CoreVersion = 103;
        boolean needUpdate = false;

        ResultSet rs;

        if ((rs = stmt.executeQuery(
            "select count(1)::integer from pg_catalog.pg_proc r inner join pg_catalog.pg_namespace l  on ( r.pronamespace = l.oid ) where 1 = 1 and l.nspname = 'h3' and r.proname = 'h3_version'"))
            .next()) {
          needUpdate = (0 == rs.getInt(1));
        }

        if (!needUpdate && (rs = stmt.executeQuery("select h3.h3_version()")).next()) {
          needUpdate = (H3CoreVersion > rs.getInt(1));
        }

        if (needUpdate) {
          String currSearchPath;

          if ((rs = stmt.executeQuery("show search_path")).next()) {
            currSearchPath = rs.getString(1);
          } else {
            throw new Exception("failed on statement [show search_path]");
          }

          stmt.execute(readResource("/h3Core.sql"));
          stmt.execute(String.format("set search_path = %1$s", currSearchPath));
          logger.info("{} - Successfully created H3 SQL-Functions", streamId);
        }
      } catch (Exception e) {
        logger.error("{} - Failed run h3 init'{}'", streamId, e);
      }
    }
  }

  private String readResource(String resource) throws IOException {
    InputStream is = PSQLRequestStreamHandler.class.getResourceAsStream(resource);
    try (BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
      return buffer.lines().collect(Collectors.joining("\n"));
    }
  }

  @Override
  protected XyzResponse processHealthCheckEvent(HealthCheckEvent event) {
    long targetResponseTime = event.getMinResponseTime() + System.currentTimeMillis();
    SQLQuery query = new SQLQuery("SELECT 1");
    try {
      //
      this.setup();

      executeQuery(query, (rs) -> null, dataSource);
      // establish a connection to the replica, if such is set.
      if (dataSource != readDataSource) {
        executeQuery(query, (rs) -> null, readDataSource);
      }

      long now = System.currentTimeMillis();
      if (now < targetResponseTime) {
        Thread.sleep(targetResponseTime - now);
      }

      return new HealthStatus().withStatus("OK");
    } catch (Exception e) {
      return new ErrorResponse().withStreamId(streamId).withError(XyzError.EXCEPTION).withErrorMessage(e.getMessage());
    }
  }

  /**
   * The result handler for a CountFeatures event.
   *
   * @param rs the result set.
   * @return the feature collection generated from the result.
   * @throws SQLException if any error occurred.
   */
  protected XyzResponse countResultSetHandler(ResultSet rs) throws SQLException {
    rs.next();
    return new CountResponse().withCount(rs.getLong(1)).withEstimated(true);
  }

  /**
   * Generates the filter query.
   */
  protected abstract SQLQuery generateSearchQuery(QueryEvent tags) throws Exception;

  /**
   * Executes the given query and returns the processed by the handler result.
   */
  <T> T executeQuery(SQLQuery query, ResultSetHandler<T> handler) throws SQLException {
    return executeQuery(query, handler, readDataSource);
  }

  /**
   * Executes the given query and returns the processed by the handler result using the provided dataSource.
   */
  private <T> T executeQuery(SQLQuery query, ResultSetHandler<T> handler, DataSource dataSource) throws SQLException {
    final long start = System.currentTimeMillis();
    try {
      final QueryRunner run = new QueryRunner(dataSource);
      query.setText(replaceVars(query.text()));
      final String queryText = query.text();
      final List<Object> queryParameters = query.parameters();
      logger.info("{} - executeQuery: {} - Parameter: {}", streamId, queryText, queryParameters);
      return run.query(queryText, handler, queryParameters.toArray());
    } finally {
      final long end = System.currentTimeMillis();
      logger.info("{} - query time: {}ms", streamId, (end - start));
    }
  }

  /**
   * Executes the given update or delete query and returns the number of deleted or updated records.
   *
   * @param query the update or delete query.
   * @return the amount of updated or deleted records.
   * @throws SQLException if any error occurred.
   */
  int executeUpdate(SQLQuery query) throws SQLException {
    final long start = System.currentTimeMillis();
    try {
      final QueryRunner run = new QueryRunner(dataSource);
      query.setText(replaceVars(query.text()));
      final String queryText = query.text();
      final List<Object> queryParameters = query.parameters();
      logger.info("{} - executeUpdate: {} - Parameter: {}", streamId, queryText, queryParameters);
      return run.update(queryText, queryParameters.toArray());
    } finally {
      final long end = System.currentTimeMillis();
      logger.info("{} - query time: {}ms", (end - start));
    }
  }

  /**
   * Creates a SQL Array of the given type.
   */
  public Array createArray(final Long[] longs, String type) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      return conn.createArrayOf(type, longs);
    }
  }

  /**
   * Creates a SQL Array of the given type.
   */
  public Array createSQLArray(final String[] strings, String type) throws SQLException {
    try (Connection conn = dataSource.getConnection()) {
      return conn.createArrayOf(type, strings);
    }
  }

  /**
   * Quote the given string so that it can be inserted into an SQL statement.
   *
   * @param text the text to escape.
   * @return the escaped text surrounded with quotes.
   */
  public String sqlQuote(final String text) {
    return text == null ? "" : '"' + text.replace("\"", "\"\"") + '"';
  }

  String replaceVars(String query) {
    return query
        .replace(VAR_SCHEMA, sqlQuote(config.schema()))
        .replace(VAR_TABLE, sqlQuote(config.table(event)));
  }

  String replaceVars(String query, Map<String, String> replacements) {
    String replaced = replaceVars(query);
    for (String key : replacements.keySet()) {
      replaced = replaced.replaceAll(PREFIX + key + SUFFIX, sqlQuote(replacements.get(key)));
    }
    return replaced;
  }
}
