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
      long sendBufferSize) throws SQLException {
    this.psqlInstance = psqlInstance;
    this.ref = new PostgresConnection(this, psqlInstance.postgresInstance, applicationName, schema, fetchSize, connTimeoutInSeconds,
        sockedReadTimeoutInSeconds, cancelSignalTimeoutInSeconds, receiveBufferSize, sendBufferSize);
  }

  /**
   * The PostgresQL instance by which this connection was created.
   */
  private @NotNull PsqlInstance psqlInstance;

  /**
   * The closable resource that wraps the real {@link PgConnection}.
   */
  private @NotNull PostgresConnection ref;

  @Override
  public @NotNull Statement createStatement() throws SQLException {
    return ref.conn().createStatement();
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql) throws SQLException {
    return ref.conn().prepareStatement(sql);
  }

  @Override
  public @NotNull CallableStatement prepareCall(@NotNull String sql) throws SQLException {
    return ref.conn().prepareCall(sql);
  }

  @Override
  public @NotNull String nativeSQL(@NotNull String sql) throws SQLException {
    return ref.conn().nativeSQL(sql);
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
    ref.conn().commit();
  }

  @Override
  public void rollback() throws SQLException {
    ref.conn().rollback();
  }

  @Override
  public void close() {
    ref.close();
  }

  @Override
  public boolean isClosed() {
    return ref.isClosed();
  }

  @Override
  public @NotNull DatabaseMetaData getMetaData() throws SQLException {
    return ref.conn().getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    if (!readOnly && ref.parent().config.readOnly) {
      throw new SQLException("The Postgres instance is a read-only instance, can't switch into write mode");
    }
    ref.conn().setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    return ref.conn().isReadOnly();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    // We do not want to switch databases!
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public @NotNull String getCatalog() throws SQLException {
    // Should return the database name to which we're connected.
    return ref.conn().getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    ref.conn().setTransactionIsolation(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    return ref.conn().getTransactionIsolation();
  }

  @Override
  public @Nullable SQLWarning getWarnings() throws SQLException {
    return ref.conn().getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    ref.conn().clearWarnings();
  }

  @Override
  public @NotNull Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    return ref.conn().createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return ref.conn().prepareStatement(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public @NotNull CallableStatement prepareCall(@NotNull String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    return ref.conn().prepareCall(sql, resultSetType, resultSetConcurrency);
  }

  @Override
  public @NotNull Map<String, Class<?>> getTypeMap() throws SQLException {
    return ref.conn().getTypeMap();
  }

  @Override
  public void setTypeMap(@NotNull Map<String, Class<?>> map) throws SQLException {
    ref.conn().setTypeMap(map);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    ref.conn().setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    return ref.conn().getHoldability();
  }

  @Override
  public @NotNull Savepoint setSavepoint() throws SQLException {
    return ref.conn().setSavepoint();
  }

  @Override
  public @NotNull Savepoint setSavepoint(@NotNull String name) throws SQLException {
    return ref.conn().setSavepoint(name);
  }

  @Override
  public void rollback(@NotNull Savepoint savepoint) throws SQLException {
    ref.conn().rollback(savepoint);
  }

  @Override
  public void releaseSavepoint(@NotNull Savepoint savepoint) throws SQLException {
    ref.conn().releaseSavepoint(savepoint);
  }

  @Override
  public @NotNull Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    return ref.conn().createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int resultSetType, int resultSetConcurrency,
      int resultSetHoldability)
      throws SQLException {
    return ref.conn().prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public @NotNull CallableStatement prepareCall(@NotNull String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    return ref.conn().prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int autoGeneratedKeys) throws SQLException {
    return ref.conn().prepareStatement(sql, autoGeneratedKeys);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, int @NotNull [] columnIndexes) throws SQLException {
    return ref.conn().prepareStatement(sql, columnIndexes);
  }

  @Override
  public @NotNull PreparedStatement prepareStatement(@NotNull String sql, @NotNull String @NotNull [] columnNames) throws SQLException {
    return ref.conn().prepareStatement(sql, columnNames);
  }

  @Override
  public @NotNull Clob createClob() throws SQLException {
    return ref.conn().createClob();
  }

  @Override
  public @NotNull Blob createBlob() throws SQLException {
    return ref.conn().createBlob();
  }

  @Override
  public @NotNull NClob createNClob() throws SQLException {
    return ref.conn().createNClob();
  }

  @Override
  public @NotNull SQLXML createSQLXML() throws SQLException {
    return ref.conn().createSQLXML();
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    return ref.conn().isValid(timeout);
  }

  @Override
  public void setClientInfo(@NotNull String name, @NotNull String value) throws SQLClientInfoException {
    try {
      ref.conn().setClientInfo(name, value);
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public void setClientInfo(@NotNull Properties properties) throws SQLClientInfoException {
    try {
      ref.conn().setClientInfo(properties);
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  @Override
  public @Nullable String getClientInfo(@NotNull String name) throws SQLException {
    return ref.conn().getClientInfo(name);
  }

  @Override
  public @NotNull Properties getClientInfo() throws SQLException {
    return ref.conn().getClientInfo();
  }

  @Override
  public @NotNull Array createArrayOf(@NotNull String typeName, @Nullable Object @NotNull [] elements) throws SQLException {
    return ref.conn().createArrayOf(typeName, elements);
  }

  @Override
  public @NotNull Struct createStruct(@NotNull String typeName, @Nullable Object @NotNull [] attributes) throws SQLException {
    return ref.conn().createStruct(typeName, attributes);
  }

  @Override
  public void setSchema(@NotNull String schema) throws SQLException {
    ref.conn().setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    return ref.conn().getSchema();
  }

  @Override
  public void abort(@NotNull Executor executor) throws SQLException {
    ref.conn().abort(executor);
  }

  @Override
  public void setNetworkTimeout(@NotNull Executor executor, int milliseconds) throws SQLException {
    ref.conn().setNetworkTimeout(executor, milliseconds);
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    return ref.conn().getNetworkTimeout();
  }

  @Override
  public <T> @NotNull T unwrap(@NotNull Class<T> iface) throws SQLException {
    if (iface.isInstance(this)) {
      return iface.cast(this);
    }
    if (iface.isInstance(ref)) {
      return iface.cast(ref);
    }
    final PgConnection conn = ref.conn();
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