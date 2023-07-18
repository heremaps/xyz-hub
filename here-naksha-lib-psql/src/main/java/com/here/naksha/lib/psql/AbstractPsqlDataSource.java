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

import static com.here.naksha.lib.psql.SQL.escapeId;

import com.here.naksha.lib.core.storage.ITransactionSettings;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper that forwards data-source calls to the underlying {@link PsqlPool pool}. This is
 * necessary, because the default way to acquire a new connection is to invoke the {@link
 * #getConnection()} method without any parameter. This means we don't have enough information to
 * initialize the new connection and not even the chance to call an initializer, so we miss setting
 * timeouts and the <a
 * href="https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH">search_path</a>.
 *
 * <p><b>In a nutshell, this class fixes data-source to add initialization to new connections.</b>
 *
 * @param <SELF> The extending class type.
 */
@SuppressWarnings("unused")
public abstract class AbstractPsqlDataSource<SELF extends AbstractPsqlDataSource<SELF>> implements DataSource {

  protected static final Logger logger = LoggerFactory.getLogger(AbstractPsqlDataSource.class);

  /**
   * Returns the default search path, the constructor calls this method ones to initialize the
   * search-path.
   *
   * @return The default search path.
   */
  protected @NotNull String defaultSearchPath() {
    return "h3,topology,public;";
  }

  /**
   * Returns the default schema, the constructor calls this method ones to initialize the
   * search-path.
   *
   * @return the default schema.
   */
  protected abstract @NotNull String defaultSchema();

  /**
   * Create a new data source for the given connection pool and application.
   *
   * @param pool the connection pool to wrap.
   * @param applicationName the application name.
   */
  protected AbstractPsqlDataSource(@NotNull PsqlPool pool, @NotNull String applicationName) {
    this.applicationName = applicationName;
    this.pool = pool;
    this.schema = defaultSchema();
    this.searchPath = defaultSearchPath();
  }

  @SuppressWarnings("unchecked")
  protected @NotNull SELF self() {
    return (SELF) this;
  }

  /** The PostgresQL connection pool to get connections from. */
  protected final @NotNull PsqlPool pool;

  /** The bound application name. */
  protected @NotNull String applicationName;

  /** The bound schema. */
  protected @NotNull String schema;

  /** The bound role; if any. */
  protected @Nullable String role;

  /** The search path to set. */
  protected @NotNull String searchPath;

  public final @NotNull PsqlPool getPool() {
    return pool;
  }

  public @NotNull PsqlPoolConfig getConfig() {
    return pool.config;
  }

  /**
   * Returns the search path, without the schema. The configured schema will always be the first
   * element in the search path.
   *
   * @return the search path, without the schema.
   */
  public @NotNull String getSearchPath() {
    return searchPath;
  }

  public void setSearchPath(@Nullable String searchPath) {
    this.searchPath = searchPath != null ? searchPath : defaultSearchPath();
  }

  public @NotNull SELF withSearchPath(@Nullable String searchPath) {
    setSearchPath(searchPath);
    return self();
  }

  public @NotNull String getApplicationName() {
    return applicationName;
  }

  public void setApplicationName(@NotNull String applicationName) {
    this.applicationName = applicationName;
  }

  public @NotNull SELF withApplicationName(@NotNull String applicationName) {
    setApplicationName(applicationName);
    return self();
  }

  public @NotNull String getSchema() {
    return schema;
  }

  public void setSchema(@Nullable String schema) {
    this.schema = schema != null ? schema : defaultSchema();
  }

  public @NotNull SELF withSchema(@NotNull String schema) {
    setSchema(schema);
    return self();
  }

  public @Nullable String getRole() {
    return role;
  }

  public void setRole(@Nullable String role) {
    this.role = role;
  }

  public @NotNull SELF withRole(@Nullable String role) {
    setRole(role);
    return self();
  }

  /**
   * Returns an initialized connection. This means the connection will have auto-commit being off, the current schema will be at the root
   * of the search path, and the correct role pre-selected. Note that these settings can be modified by overriding the
   * {@link #initConnection(Connection,ITransactionSettings)} or {@link #initSession(StringBuilder,ITransactionSettings)} methods.
   *
   * @return the initialized connection.
   * @throws SQLException if any error happened while initializing the connection.
   */
  @Override
  public final Connection getConnection() throws SQLException {
    return initConnection(pool.dataSource.getConnection());
  }

  /**
   * Returns an initialized connection. This means the connection will have auto-commit being off, the current schema will be at the root
   * of the search path, and the correct role pre-selected. Note that these settings can be modified by overriding the
   * {@link #initConnection(Connection,ITransactionSettings)} or {@link #initSession(StringBuilder,ITransactionSettings)} methods.
   *
   * @param settings Transaction settings.
   * @return the initialized connection.
   * @throws SQLException if any error happened while initializing the connection.
   */
  public Connection getConnection(@NotNull ITransactionSettings settings) throws SQLException {
    return initConnection(pool.dataSource.getConnection(), settings);
  }

  /**
   * This method is not supported an will always throw an {@link SQLException} when being called.
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
   * @return New transaction settings.
   */
  public @NotNull ITransactionSettings createSettings() {
    return new PsqlTransactionSettings(pool.config.stmtTimeout, pool.config.lockTimeout);
  }

  /**
   * The default initializer for connections.
   *
   * @param conn The connection.
   * @return the connection.
   * @throws SQLException if the init failed.
   */
  public final @NotNull Connection initConnection(@NotNull Connection conn) throws SQLException {
    return initConnection(conn, null);
  }

  /**
   * The default initializer for connections.
   *
   * @param conn The connection.
   * @param settings The default connection settings.
   * @return the connection.
   * @throws SQLException if the init failed.
   */
  public @NotNull Connection initConnection(@NotNull Connection conn, @Nullable ITransactionSettings settings)
      throws SQLException {
    conn.setAutoCommit(false);
    try (final Statement stmt = conn.createStatement()) {
      final StringBuilder sb = new StringBuilder();
      initSession(sb, settings);
      final String sql = sb.toString();
      logger.debug("{} - Init connection: {}", applicationName, sql);
      stmt.execute(sql);
      conn.commit();
    }
    return conn;
  }

  /**
   * Generates the initialization query.
   *
   * <p><b>Note</b>: If SET (or equivalently SET SESSION) is issued within a transaction that is
   * later aborted, the effects of the SET command disappear when the transaction is rolled back.
   * Once the surrounding transaction is committed, the effects will persist until the end of the
   * session, unless overridden by another SET.
   *
   * <p>From the <a href="https://www.postgresql.org/docs/current/sql-set.html">PostgresQL
   * documentation</a>. Therefore, the query that is created in this method is committed, because we
   * do not know how many transactions are done with the connection and we want all of them to use
   * the same settings.
   *
   * @param settings The transaction settings; if any.
   * @param sb The string builder in which to create the query.
   * @throws SQLException If any error occurred.
   */
  protected void initSession(@NotNull StringBuilder sb, @Nullable ITransactionSettings settings) throws SQLException {
    sb.append("SET SESSION application_name TO '").append(applicationName).append("';\n");
    sb.append("SET SESSION work_mem TO '256 MB';\n");
    sb.append("SET SESSION enable_seqscan TO OFF;\n");
    // sb.append("SET SESSION enable_bitmapscan TO OFF;\n");
    sb.append("SET SESSION statement_timeout TO ")
        .append(
            settings != null
                ? settings.getStatementTimeout(TimeUnit.MICROSECONDS)
                : pool.config.stmtTimeout)
        .append(";\n");
    sb.append("SET SESSION lock_timeout TO ")
        .append(settings != null ? settings.getLockTimeout(TimeUnit.MILLISECONDS) : pool.config.lockTimeout)
        .append(";\n");
    sb.append("SET SESSION search_path TO ");
    if (!searchPath.contains(schema)) {
      sb.append('"').append(schema).append('"').append(',');
    }
    sb.append(searchPath).append(";\n");
    if (role != null && !role.equals(pool.config.user)) {
      sb.append("SET SESSION ROLE ");
      escapeId(sb, role);
    } else {
      sb.append("RESET ROLE");
    }
    sb.append(";\n");
  }
}
