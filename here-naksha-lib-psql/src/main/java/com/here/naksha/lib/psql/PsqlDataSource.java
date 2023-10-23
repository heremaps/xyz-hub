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
package com.here.naksha.lib.psql;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.psql.SQL.close_literal;
import static com.here.naksha.lib.psql.SQL.open_literal;
import static com.here.naksha.lib.psql.SQL.write_literal;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.exceptions.Unauthorized;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import javax.sql.DataSource;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper that forwards data-source calls to the underlying {@link PsqlPool pool}. This is necessary, because the default way to acquire
 * a new connection is to invoke the {@link #getConnection()} method without any parameter. This means we don't have enough information to
 * initialize the new connection and not even the chance to call an initializer, so we miss setting timeouts and the <a
 * href="https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH">search_path</a>. Additionally, there is a rare situation
 * in which (when a failover happens) the data-source need to be replaced, but code may keep references to this data source, so we need to
 * exchange the data-source that is wrapped.
 *
 * <p><b>In a nutshell, this class fixes data-source to add initialization to new connections.</b>
 */
@SuppressWarnings("unused")
public class PsqlDataSource implements DataSource {

  private static final Logger logger = LoggerFactory.getLogger(PsqlDataSource.class);

  /**
   * Create a new data source for the given connection pool and application.
   *
   * @param config the configuration to use.
   */
  public PsqlDataSource(@NotNull PsqlConfig config) {
    this.pool = PsqlPool.get(config);
    this.config = config;
    this.applicationName = config.appName;
    this.searchPath = searchPath(config.searchPath);
  }

  /**
   * The PostgresQL connection pool to get connections from.
   */
  private final @NotNull PsqlPool pool;

  /**
   * The PostgresQL configuration.
   */
  private final @NotNull PsqlConfig config;

  /**
   * The bound application name.
   */
  private @NotNull String applicationName;

  /**
   * The search path to set.
   */
  private @NotNull String searchPath;

  public final @NotNull PsqlPool getPool() {
    return pool;
  }

  public @NotNull PsqlConfig getConfig() {
    return config;
  }

  /**
   * Returns the search path, without the schema. The configured schema will always be the first element in the search path.
   *
   * @return the search path, without the schema.
   */
  public @NotNull String getSearchPath() {
    return searchPath;
  }

  private @NotNull String searchPath(@Nullable String searchPath) {
    final StringBuilder sb = new StringBuilder();
    sb.append(config.schema);
    if (searchPath != null && !searchPath.isEmpty()) {
      // TODO: Verify that the search-path does not contain illegal characters, this is an attack vector!
      // sb.append(',').append(config.searchPath);
    }
    sb.append(",topology,public;");
    return sb.toString();
  }

  public void setSearchPath(@NotNull String searchPath) {
    this.searchPath = searchPath(searchPath);
  }

  public @NotNull PsqlDataSource withSearchPath(@Nullable String searchPath) {
    this.searchPath = searchPath(searchPath);
    return this;
  }

  public @NotNull String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(@NotNull String applicationName) {
    this.applicationName = applicationName;
  }

  public @NotNull PsqlDataSource withApplicationName(@NotNull String applicationName) {
    setApplicationName(applicationName);
    return this;
  }

  public @NotNull String getSchema() {
    return config.schema;
  }

  /**
   * Returns an initialized connection. This means the connection will have auto-commit being off, the current schema will be at the root of
   * the search path, and the {@code naksha_start_session} method will have been invoked. Note that these settings can be modified by
   * overriding the {@link #initConnection(Connection, NakshaContext)} and/or {@link #initSession(StringBuilder, NakshaContext)} methods.
   *
   * @return the initialized connection.
   * @throws SQLException If any error happened while initializing the connection.
   * @throws Unauthorized If the current {@link NakshaContext} does not have a valid application-identifier.
   */
  @Override
  public final @NotNull Connection getConnection() throws SQLException {
    return initConnection(pool.dataSource.getConnection(), NakshaContext.currentContext());
  }

  /**
   * Returns an initialized connection. This means the connection will have auto-commit being off, the current schema will be at the root of
   * the search path. If a context is given, {@code naksha_start_session} method will have been invoked. Note that these settings can be
   * modified by overriding the {@link #initConnection(Connection, NakshaContext)} and/or {@link #initSession(StringBuilder, NakshaContext)}
   * methods.
   *
   * @param context If a context is given, it must have a valid application-identifier and causes {@code naksha_start_session} to be
   *                invoked.
   * @return the initialized connection.
   * @throws SQLException if any error happened while initializing the connection.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull Connection getConnection(@Nullable NakshaContext context) throws SQLException {
    return initConnection(pool.dataSource.getConnection(), context);
  }

  /**
   * This method is not supported and will always throw an {@link SQLException} when being called.
   *
   * @throws SQLException in any case.
   */
  @Deprecated
  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    throw new SQLException("Not supported operation");
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return pool.dataSource.getLogWriter();
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    pool.dataSource.setLogWriter(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    pool.dataSource.setLoginTimeout(seconds);
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return pool.dataSource.getLoginTimeout();
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return pool.dataSource.getParentLogger();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return pool.dataSource.unwrap(iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return pool.dataSource.isWrapperFor(iface);
  }

  /**
   * Create default transaction settings.
   *
   * @return New transaction settings.
   */
  @Deprecated
  public @NotNull ITransactionSettings createSettings() {
    return new PsqlTransactionSettings(pool.config.stmtTimeout, pool.config.lockTimeout);
  }

  /**
   * The default initializer for connections.
   *
   * @param conn    The connection.
   * @param context If a context is given, it must have a valid application-identifier and causes {@code naksha_start_session} to be
   *                invoked.
   * @return the connection.
   * @throws SQLException If the initialization failed.
   * @throws Unauthorized If a context is given and does not have a valid application-identifier.
   */
  @SuppressWarnings("SqlSourceToSinkFlow")
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull Connection initConnection(@NotNull Connection conn, @Nullable NakshaContext context) {
    try {
      conn.setAutoCommit(false);
      try (final Statement stmt = conn.createStatement()) {
        final StringBuilder sb = new StringBuilder();
        initSession(sb, context);
        final String sql = sb.toString();
        logger.debug("{} - Init connection: {}", applicationName, sql);
        stmt.execute(sql);
        conn.commit();
      }
      return conn;
    } catch (Exception e) {
      throw unchecked(e);
    }
  }

  /**
   * Generates the initialization query.
   *
   * <p><b>Note</b>: If SET (or equivalently SET SESSION) is issued within a transaction that is later aborted, the effects of the SET
   * command disappear when the transaction is rolled back. Once the surrounding transaction is committed, the effects will persist until
   * the end of the session, unless overridden by another SET, see the
   * <a href="https://www.postgresql.org/docs/current/sql-set.html">PostgresQL documentation</a>. Therefore, the query that is created in
   * this method is committed, because we do not know how many transactions are done with the connection, and we want all of them to use the
   * same settings.
   *
   * @param sb      The string builder in which to create the query.
   * @param context If a context is given, it must have a valid application-identifier and causes {@code naksha_start_session} to be
   *                invoked.
   * @throws Unauthorized If a context without application-identifier given.
   */
  protected void initSession(@NotNull StringBuilder sb, @Nullable NakshaContext context) {
    // TODO: We need to escape the application name.
    sb.append("SET SESSION application_name TO '").append(applicationName).append("';\n");
    sb.append("SET SESSION work_mem TO '256 MB';\n");
    sb.append("SET SESSION enable_seqscan TO OFF;\n");
    sb.append("SET SESSION statement_timeout TO ")
        .append(config.stmtTimeout)
        .append(";\n");
    sb.append("SET SESSION lock_timeout TO ").append(config.lockTimeout).append(";\n");
    sb.append("SET SESSION search_path TO ").append(searchPath).append(";\n");
    // sb.append("SELECT naksha_init_plv8();");
    if (context != null) {
      sb.append("SELECT naksha_tx_start(");
      open_literal(sb);
      write_literal(context.getAppId(), sb);
      close_literal(sb);
      sb.append(',');
      String author = context.getAuthor();
      if (author != null) {
        open_literal(sb);
        write_literal(author, sb);
        close_literal(sb);
      } else {
        sb.append("null");
      }
      sb.append(",false);");
    }
  }
}
