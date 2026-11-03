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

package com.here.xyz.util.db.datasource;

import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.ds.PGSimpleDataSource;

/**
 * A {@link DataSourceProvider} that opens at most one physical connection to the writer and hands it out for all queries.
 * Closing a connection obtained from it has no effect, closing the provider closes the physical connection.
 */
public class SingleConnectionDataSources extends DataSourceProvider {
  private static final Logger logger = LogManager.getLogger(SingleConnectionDataSources.class);
  private final PGSimpleDataSource physicalDataSource;
  private final DataSource dataSource = new SingleConnectionDataSource();
  private Connection connection;
  private Connection connectionProxy;
  private int openedConnectionCount;

  /**
   * @param dbSettings The settings to connect with
   * @param checkTimeoutSeconds The connect timeout, login timeout and the initial statement timeout
   * @param socketTimeoutSeconds The socket read timeout
   */
  public SingleConnectionDataSources(DatabaseSettings dbSettings, int checkTimeoutSeconds, int socketTimeoutSeconds) {
    super(dbSettings);
    physicalDataSource = createDataSource(checkTimeoutSeconds, socketTimeoutSeconds);
  }

  private PGSimpleDataSource createDataSource(int checkTimeoutSeconds, int socketTimeoutSeconds) {
    PGSimpleDataSource ds = new PGSimpleDataSource();
    ds.setServerNames(new String[] {dbSettings.getHost()});
    ds.setPortNumbers(new int[] {dbSettings.getPort()});
    ds.setDatabaseName(dbSettings.getDb());
    ds.setUser(dbSettings.getUser());
    ds.setPassword(dbSettings.getPassword());
    ds.setApplicationName(dbSettings.getApplicationName());
    ds.setCurrentSchema(dbSettings.getSchema());
    ds.setConnectTimeout(checkTimeoutSeconds);
    ds.setLoginTimeout(checkTimeoutSeconds);
    ds.setSocketTimeout(socketTimeoutSeconds);
    ds.setOptions("-c statement_timeout=" + checkTimeoutSeconds * 1_000);
    ds.setTcpKeepAlive(true);
    return ds;
  }

  private synchronized Connection getConnection() throws SQLException {
    if (connectionProxy == null) {
      Connection physicalConnection = physicalDataSource.getConnection();
      openedConnectionCount++;
      connection = physicalConnection;
      connectionProxy = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
          (proxy, method, args) -> {
            if ("close".equals(method.getName()) && method.getParameterCount() == 0)
              return null;
            try {
              return method.invoke(physicalConnection, args);
            }
            catch (InvocationTargetException e) {
              throw e.getCause();
            }
          });
    }
    return connectionProxy;
  }

  /**
   * @return The number of physical connections this provider has opened
   */
  public synchronized int getOpenedConnectionCount() {
    return openedConnectionCount;
  }

  @Override
  public DataSource getReader() {
    return dataSource;
  }

  @Override
  public DataSource getWriter() {
    return dataSource;
  }

  @Override
  public synchronized void close() {
    if (connection == null)
      return;
    try {
      connection.close();
    }
    catch (SQLException e) {
      logger.warn("Error closing the connection to DB {}.", dbSettings.getId(), e);
    }
    connection = null;
    connectionProxy = null;
  }

  private class SingleConnectionDataSource implements DataSource {

    @Override
    public Connection getConnection() throws SQLException {
      return SingleConnectionDataSources.this.getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
      throw new SQLFeatureNotSupportedException("Connections can only be opened with the configured credentials.");
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
      return physicalDataSource.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
      physicalDataSource.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
      physicalDataSource.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
      return physicalDataSource.getLoginTimeout();
    }

    @Override
    public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
      return physicalDataSource.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
      return physicalDataSource.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
      return physicalDataSource.isWrapperFor(iface);
    }
  }
}
