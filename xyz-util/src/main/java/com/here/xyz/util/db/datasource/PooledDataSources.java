/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import com.here.xyz.util.db.DatabaseSettings;
import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PooledDataSources extends DataSourceProvider {

  private static final Logger logger = LogManager.getLogger();
  private static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()";
  protected DatabaseSettings dbSettings;
  private volatile ComboPooledDataSource reader;
  private volatile ComboPooledDataSource writer;

  public PooledDataSources(DatabaseSettings dbSettings) {
    this.dbSettings = dbSettings;
    //FIXME: find a better way to assign that value in a non-static way (error-prone if s.b. changes the value)
    XyzConnectionCustomizer.statementTimeoutSettings = dbSettings.getStatementTimeoutSeconds();
  }

  public DatabaseSettings getDatabaseSettings() {
    return dbSettings;
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
    private static int statementTimeoutSettings;

      private String getSchema(String parentDataSourceIdentityToken) {
          return (String) extensionsForToken(parentDataSourceIdentityToken).get(C3P0EXT_CONFIG_SCHEMA);
      }

      public void onAcquire(Connection c, String pdsIdt) {
          String schema = getSchema(pdsIdt);  // config.schema();
          QueryRunner runner = new QueryRunner();
          try {
              runner.execute(c, "SET enable_seqscan = off;");
              runner.execute(c, "SET statement_timeout = " + (statementTimeoutSettings * 1000) + " ;");
              runner.execute(c, "SET search_path=" + schema + ",h3,public,topology;");
          }
          catch (SQLException e) {
              logger.error("Failed to initialize connection " + c + " [" + pdsIdt + "] : {}", e);
          }
      }
  }

  public static ComboPooledDataSource getComboPooledDataSource(DatabaseSettings dbSettings, boolean useReplica) {
    return getComboPooledDataSource(dbSettings.getJdbcUrl(useReplica), useReplica ? dbSettings.getReplicaUser() : dbSettings.getUser(),
        dbSettings.getPassword(), dbSettings.getSchema(), dbSettings.getDbMinPoolSize(), dbSettings.getDbMaxPoolSize(),
        dbSettings.getDbInitialPoolSize(), dbSettings.getDbAcquireRetryAttempts(), dbSettings.getDbAcquireIncrement(),
        dbSettings.getDbCheckoutTimeout(), dbSettings.getDbMaxIdleTime(), dbSettings.isDbTestConnectionOnCheckout());
  }

  private static ComboPooledDataSource getComboPooledDataSource(String jdbcUrl, String user, String password, String schema,
      int minPoolSize, int maxPoolSize, int initialPoolSize, int acquireRetryAttempts, int acquireIncrement, int checkoutTimeout,
      int maxIdleTime, boolean testConnectionOnCheckout) {

    final ComboPooledDataSource cpds = new ComboPooledDataSource();
    cpds.setJdbcUrl(jdbcUrl);
    cpds.setUser(user);
    cpds.setPassword(password);
    cpds.setInitialPoolSize(initialPoolSize);
    cpds.setMinPoolSize(minPoolSize);
    cpds.setMaxPoolSize(maxPoolSize);
    cpds.setAcquireRetryAttempts(acquireRetryAttempts);
    cpds.setAcquireIncrement(acquireIncrement);
    cpds.setCheckoutTimeout(checkoutTimeout);
    cpds.setMaxIdleTime(maxIdleTime);
    cpds.setTestConnectionOnCheckout(testConnectionOnCheckout);

    cpds.setConnectionCustomizerClassName(XyzConnectionCustomizer.class.getName());
    cpds.setExtensions(Collections.singletonMap(C3P0EXT_CONFIG_SCHEMA, schema));

    return cpds;
  }
}
