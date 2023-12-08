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

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.jdbc.PgConnection;

/**
 * A PostgresQL connection proxy from a connection pool of a {@link PsqlInstance}.
 */
public class PsqlConnection implements Connection {

  /**
   * Creates a new PSQL connection proxy and acquire a new PG connection.
   *
   * @param postgresInstance            The Postgres instance.
   * @param connTimeoutInMillis         The connection timeout in milliseconds, will be set as well for socket read-timeout.
   * @param cancelSignalTimeoutInMillis The
   * @param receiveBufferSize           The receive-buffer size in byte.
   * @param sendBufferSize              The send-buffer size in byte.
   * @throws SQLException If establishing the connection failed.
   */
  PsqlConnection(
      @NotNull PostgresInstance postgresInstance,
      long connTimeoutInMillis,
      long cancelSignalTimeoutInMillis,
      long receiveBufferSize,
      long sendBufferSize)
      throws SQLException {
    this.postgresConnection = new PostgresConnection(
        this,
        postgresInstance,
        connTimeoutInMillis,
        connTimeoutInMillis,
        cancelSignalTimeoutInMillis,
        receiveBufferSize,
        sendBufferSize);
  }

  /**
   * Creates a new PSQL connection proxy for an existing PG connection.
   *
   * @param postgresInstance The Postgres instance.
   * @param pgConnection     The PG connection.
   * @throws SQLException If assigning the connection failed.
   */
  PsqlConnection(@NotNull PostgresInstance postgresInstance, @NotNull PgConnection pgConnection) throws SQLException {
    this.postgresConnection = new PostgresConnection(this, postgresInstance, pgConnection);
  }

  /**
   * The closable resource that wraps the real {@link PgConnection}.
   */
  final @NotNull PostgresConnection postgresConnection;

  @Override
  public @NotNull Statement createStatement() throws SQLException {
    return postgresConnection.get().createStatement();
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql) throws SQLException {
    return postgresConnection.get().prepareStatement(sql);
  }

  @Override
  public @NotNull CallableStatement prepareCall(@NotNull String sql) throws SQLException {
    return postgresConnection.get().prepareCall(sql);
  }

  @Override
  public @NotNull String nativeSQL(@NotNull String sql) throws SQLException {
    return postgresConnection.get().nativeSQL(sql);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    if (autoCommit) {
      throw new SQLFeatureNotSupportedException();
    }
  }

  @Override
  public boolean getAutoCommit() {
    return true;
  }

  @Override
  public void commit() throws SQLException {
    postgresConnection.get().commit();
  }

  @Override
  public void rollback() throws SQLException {
    postgresConnection.get().rollback();
  }

  @Override
  public void close() {
    postgresConnection.close();
  }

  @Override
  public boolean isClosed() {
    return postgresConnection.isClosed();
  }

  @Override
  public @NotNull DatabaseMetaData getMetaData() throws SQLException {
    return postgresConnection.get().getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (!readOnly && postgresConnection.parent().config.readOnly) {
      throw new SQLException("The Postgres instance is a read-only instance, can't switch into write mode");
    }
    postgresConnection.get().setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return postgresConnection.get().isReadOnly();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    // We do not want to switch databases!
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public @NotNull String getCatalog() throws SQLException {
    // Should return the database name to which we're connected.
    return postgresConnection.get().getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    postgresConnection.get().setTransactionIsolation(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return postgresConnection.get().getTransactionIsolation();
  }

  @Override
  public @Nullable SQLWarning getWarnings() throws SQLException {
    return postgresConnection.get().getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    postgresConnection.get().clearWarnings();
  }

  @Override
  public @NotNull Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return postgresConnection.get().createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return postgresConnection.get().prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public @NotNull CallableStatement prepareCall(@NotNull String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return postgresConnection.get().prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public @NotNull Map<String, Class<?>> getTypeMap() throws SQLException {
    return postgresConnection.get().getTypeMap();
  }

  @Override
  public void setTypeMap(@NotNull Map<String, Class<?>> map) throws SQLException {
    postgresConnection.get().setTypeMap(map);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    postgresConnection.get().setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    return postgresConnection.get().getHoldability();
  }

  @Override
  public @NotNull Savepoint setSavepoint() throws SQLException {
    return postgresConnection.get().setSavepoint();
  }

  @Override
  public @NotNull Savepoint setSavepoint(@NotNull String name) throws SQLException {
    return postgresConnection.get().setSavepoint(name);
  }

  @Override
  public void rollback(@NotNull Savepoint savepoint) throws SQLException {
    postgresConnection.get().rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(@NotNull Savepoint savepoint) throws SQLException {
    postgresConnection.get().releaseSavepoint(savepoint);
  }

  @Override
  public @NotNull Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return postgresConnection.get().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(
      @NotNull String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return postgresConnection
        .get()
        .prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public @NotNull CallableStatement prepareCall(
      @NotNull String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return postgresConnection.get().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int autoGeneratedKeys) throws SQLException {
    return postgresConnection.get().prepareStatement(sql, autoGeneratedKeys);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int @NotNull [] columnIndexes)
      throws SQLException {
    return postgresConnection.get().prepareStatement(sql, columnIndexes);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, @NotNull String @NotNull [] columnNames)
      throws SQLException {
    return postgresConnection.get().prepareStatement(sql, columnNames);
  }

  @Override
  public @NotNull Clob createClob() throws SQLException {
    return postgresConnection.get().createClob();
  }

  @Override
  public @NotNull Blob createBlob() throws SQLException {
    return postgresConnection.get().createBlob();
  }

  @Override
  public @NotNull NClob createNClob() throws SQLException {
    return postgresConnection.get().createNClob();
  }

  @Override
  public @NotNull SQLXML createSQLXML() throws SQLException {
    return postgresConnection.get().createSQLXML();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return postgresConnection.get().isValid(timeout);
  }

  @Override
  public void setClientInfo(@NotNull String name, @NotNull String value) throws SQLClientInfoException {
    try {
      postgresConnection.get().setClientInfo(name, value);
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public void setClientInfo(@NotNull Properties properties) throws SQLClientInfoException {
    try {
      postgresConnection.get().setClientInfo(properties);
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @Nullable String getClientInfo(@NotNull String name) throws SQLException {
    return postgresConnection.get().getClientInfo(name);
  }

  @Override
  public @NotNull Properties getClientInfo() throws SQLException {
    return postgresConnection.get().getClientInfo();
  }

  @Override
  public @NotNull Array createArrayOf(@NotNull String typeName, @Nullable Object @NotNull [] elements)
      throws SQLException {
    return postgresConnection.get().createArrayOf(typeName, elements);
  }

  @Override
  public @NotNull Struct createStruct(@NotNull String typeName, @Nullable Object @NotNull [] attributes)
      throws SQLException {
    return postgresConnection.get().createStruct(typeName, attributes);
  }

  @Override
  public void setSchema(@NotNull String schema) throws SQLException {
    postgresConnection.get().setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    return postgresConnection.get().getSchema();
  }

  @Override
  public void abort(@NotNull Executor executor) throws SQLException {
    postgresConnection.get().abort(executor);
  }

  @Override
  public void setNetworkTimeout(@NotNull Executor executor, int milliseconds) throws SQLException {
    postgresConnection.get().setNetworkTimeout(executor, milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return postgresConnection.get().getNetworkTimeout();
  }

  @Override
  public <T> @NotNull T unwrap(@NotNull Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    if (iface.isInstance(postgresConnection)) {
      return iface.cast(postgresConnection);
    }
    final PgConnection conn = postgresConnection.get();
    if (iface.isInstance(conn)) {
      return iface.cast(conn);
    }
    throw new SQLException("The interface " + iface.getName() + " is not wrapped");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    try {
      unwrap(iface);
      return true;
    } catch (SQLException ignore) {
      return false;
    }
  }
}
