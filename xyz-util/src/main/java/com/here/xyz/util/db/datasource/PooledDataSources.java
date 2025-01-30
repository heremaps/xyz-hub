/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.util.db.datasource;

import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PooledDataSources extends DataSourceProvider {
  private static final Logger logger = LogManager.getLogger();
  private static final String EXTENDED_CONNECTION_SETTINGS = "extendedConnectionSettings";
  private volatile ComboPooledDataSource reader;
  private volatile ComboPooledDataSource writer;

  public PooledDataSources(DatabaseSettings dbSettings) {
    super(dbSettings);
  }

  @Override
  public DataSource getReader() {
    if (dbSettings.getReplicaHost() == null)
      return getWriter();
    if (reader == null)
      reader = getComboPooledDataSource(dbSettings, true);
    return reader;
  }

  @Override
  public DataSource getWriter() {
    if (writer == null)
      writer = getComboPooledDataSource(dbSettings, false);
    return writer;
  }

  private static ComboPooledDataSource getComboPooledDataSource(DatabaseSettings dbSettings, boolean useReplica) {
    final ComboPooledDataSource cpds = new ComboPooledDataSource();
    cpds.setJdbcUrl(dbSettings.getJdbcUrl(useReplica));
    cpds.setUser(useReplica ? dbSettings.getReplicaUser() : dbSettings.getUser());
    cpds.setPassword(dbSettings.getPassword());
    cpds.setInitialPoolSize(dbSettings.getDbInitialPoolSize());
    cpds.setMinPoolSize(dbSettings.getDbMinPoolSize());
    cpds.setMaxPoolSize(dbSettings.getDbMaxPoolSize());
    cpds.setAcquireRetryAttempts(dbSettings.getDbAcquireRetryAttempts());
    cpds.setAcquireIncrement(dbSettings.getDbAcquireIncrement());
    cpds.setCheckoutTimeout(dbSettings.getDbCheckoutTimeout());
    cpds.setMaxIdleTime(dbSettings.getDbMaxIdleTime());
    cpds.setTestConnectionOnCheckout(dbSettings.isDbTestConnectionOnCheckout());

    cpds.setConnectionCustomizerClassName(XyzConnectionCustomizer.class.getName());
    cpds.setExtensions(Map.of(EXTENDED_CONNECTION_SETTINGS, new ExtendedConnectionSettings(dbSettings.getSchema(),
        dbSettings.getSearchPath() == null ? List.of() : dbSettings.getSearchPath(), dbSettings.getStatementTimeoutSeconds())));

    return cpds;
  }

  @Override
  public void close() throws Exception {
    if (reader != null)
      reader.close();
    if (writer != null)
      writer.close();
  }

  /**
   * Handles initialization per db connection
   */
  public static class XyzConnectionCustomizer extends AbstractConnectionCustomizer {

    public void onAcquire(Connection connection, String connectionId) {
      ExtendedConnectionSettings extendedSettings = getExtendedSettings(connectionId);
      List<String> enrichedSearchPath = new ArrayList<>(List.of(extendedSettings.currentSchema, "h3", "public", "topology"));
      enrichedSearchPath.addAll(extendedSettings.searchPath);
      final String compiledSearchPath = enrichedSearchPath.stream().map(schema -> "\"" + schema + "\"")
          .collect(Collectors.joining(", "));

      QueryRunner runner = new QueryRunner();
      try {
        runner.execute(connection, "SET enable_seqscan = off;");
        runner.execute(connection, "SET statement_timeout = " + (extendedSettings.statementTimeoutSeconds * 1000) + ";");
        runner.execute(connection, "SET search_path = " + compiledSearchPath + ";");
      }
      catch (SQLException e) {
        logger.error("Failed to initialize connection " + connection + " [" + connectionId + "] : {}", e);
      }
    }

    private ExtendedConnectionSettings getExtendedSettings(String connectionId) {
      return (ExtendedConnectionSettings) extensionsForToken(connectionId).get(EXTENDED_CONNECTION_SETTINGS);
    }
  }

  private record ExtendedConnectionSettings(String currentSchema, List<String> searchPath, int statementTimeoutSeconds) {}
}
