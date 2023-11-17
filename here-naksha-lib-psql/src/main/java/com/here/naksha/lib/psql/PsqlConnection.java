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

  // TODO: We need another constructor that can reused a pooled PgConnection!

  PsqlConnection(
      @NotNull PsqlInstance psqlInstance,
      @NotNull String applicationName,
      @NotNull String schema,
      int fetchSize,
      long connTimeoutInSeconds,
      long sockedReadTimeoutInSeconds,
      long cancelSignalTimeoutInSeconds,
      long receiveBufferSize,
      long sendBufferSize)
      throws SQLException {
    this.psqlInstance = psqlInstance;
    this.connection = new PostgresConnection(
        this,
        psqlInstance.postgresInstance,
        applicationName,
        schema,
        fetchSize,
        connTimeoutInSeconds,
        sockedReadTimeoutInSeconds,
        cancelSignalTimeoutInSeconds,
        receiveBufferSize,
        sendBufferSize);
  }

  /**
   * The PostgresQL instance by which this connection was created.
   */
  private final @NotNull PsqlInstance psqlInstance;

  /**
   * The closable resource that wraps the real {@link PgConnection}.
   */
  final @NotNull PostgresConnection connection;

  @Override
  public @NotNull Statement createStatement() throws SQLException {
    return connection.get().createStatement();
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql) throws SQLException {
    return connection.get().prepareStatement(sql);
  }

  @Override
  public @NotNull CallableStatement prepareCall(@NotNull String sql) throws SQLException {
    return connection.get().prepareCall(sql);
  }

  @Override
  public @NotNull String nativeSQL(@NotNull String sql) throws SQLException {
    return connection.get().nativeSQL(sql);
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
    connection.get().commit();
  }

  @Override
  public void rollback() throws SQLException {
    connection.get().rollback();
  }

  @Override
  public void close() {
    connection.close();
  }

  @Override
  public boolean isClosed() {
    return connection.isClosed();
  }

  @Override
  public @NotNull DatabaseMetaData getMetaData() throws SQLException {
    return connection.get().getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (!readOnly && connection.parent().config.readOnly) {
      throw new SQLException("The Postgres instance is a read-only instance, can't switch into write mode");
    }
    connection.get().setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return connection.get().isReadOnly();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    // We do not want to switch databases!
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public @NotNull String getCatalog() throws SQLException {
    // Should return the database name to which we're connected.
    return connection.get().getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    connection.get().setTransactionIsolation(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return connection.get().getTransactionIsolation();
  }

  @Override
  public @Nullable SQLWarning getWarnings() throws SQLException {
    return connection.get().getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    connection.get().clearWarnings();
  }

  @Override
  public @NotNull Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return connection.get().createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return connection.get().prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public @NotNull CallableStatement prepareCall(@NotNull String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    return connection.get().prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public @NotNull Map<String, Class<?>> getTypeMap() throws SQLException {
    return connection.get().getTypeMap();
  }

  @Override
  public void setTypeMap(@NotNull Map<String, Class<?>> map) throws SQLException {
    connection.get().setTypeMap(map);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    connection.get().setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    return connection.get().getHoldability();
  }

  @Override
  public @NotNull Savepoint setSavepoint() throws SQLException {
    return connection.get().setSavepoint();
  }

  @Override
  public @NotNull Savepoint setSavepoint(@NotNull String name) throws SQLException {
    return connection.get().setSavepoint(name);
  }

  @Override
  public void rollback(@NotNull Savepoint savepoint) throws SQLException {
    connection.get().rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(@NotNull Savepoint savepoint) throws SQLException {
    connection.get().releaseSavepoint(savepoint);
  }

  @Override
  public @NotNull Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return connection.get().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(
      @NotNull String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return connection.get().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public @NotNull CallableStatement prepareCall(
      @NotNull String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return connection.get().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int autoGeneratedKeys) throws SQLException {
    return connection.get().prepareStatement(sql, autoGeneratedKeys);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int @NotNull [] columnIndexes)
      throws SQLException {
    return connection.get().prepareStatement(sql, columnIndexes);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, @NotNull String @NotNull [] columnNames)
      throws SQLException {
    return connection.get().prepareStatement(sql, columnNames);
  }

  @Override
  public @NotNull Clob createClob() throws SQLException {
    return connection.get().createClob();
  }

  @Override
  public @NotNull Blob createBlob() throws SQLException {
    return connection.get().createBlob();
  }

  @Override
  public @NotNull NClob createNClob() throws SQLException {
    return connection.get().createNClob();
  }

  @Override
  public @NotNull SQLXML createSQLXML() throws SQLException {
    return connection.get().createSQLXML();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return connection.get().isValid(timeout);
  }

  @Override
  public void setClientInfo(@NotNull String name, @NotNull String value) throws SQLClientInfoException {
    try {
      connection.get().setClientInfo(name, value);
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public void setClientInfo(@NotNull Properties properties) throws SQLClientInfoException {
    try {
      connection.get().setClientInfo(properties);
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @Nullable String getClientInfo(@NotNull String name) throws SQLException {
    return connection.get().getClientInfo(name);
  }

  @Override
  public @NotNull Properties getClientInfo() throws SQLException {
    return connection.get().getClientInfo();
  }

  @Override
  public @NotNull Array createArrayOf(@NotNull String typeName, @Nullable Object @NotNull [] elements)
      throws SQLException {
    return connection.get().createArrayOf(typeName, elements);
  }

  @Override
  public @NotNull Struct createStruct(@NotNull String typeName, @Nullable Object @NotNull [] attributes)
      throws SQLException {
    return connection.get().createStruct(typeName, attributes);
  }

  @Override
  public void setSchema(@NotNull String schema) throws SQLException {
    connection.get().setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    return connection.get().getSchema();
  }

  @Override
  public void abort(@NotNull Executor executor) throws SQLException {
    connection.get().abort(executor);
  }

  @Override
  public void setNetworkTimeout(@NotNull Executor executor, int milliseconds) throws SQLException {
    connection.get().setNetworkTimeout(executor, milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return connection.get().getNetworkTimeout();
  }

  @Override
  public <T> @NotNull T unwrap(@NotNull Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    if (iface.isInstance(connection)) {
      return iface.cast(connection);
    }
    final PgConnection conn = connection.get();
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
