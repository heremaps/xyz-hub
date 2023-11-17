package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.util.CloseableResource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;

/**
 * A PostgresQL connection pool.
 */
class PostgresPool extends CloseableResource<PostgresPool> {

  PostgresPool(@NotNull Object proxy, @NotNull PsqlInstanceConfig masterConfig) {
    super(proxy, null);
    this.configs = new ConcurrentHashMap<>();
    this.configs.put(MASTER, masterConfig);
    this.logWriter = new AtomicReference<>(slf4jLogWriter);
  }

  /**
   * The unique identifier of the master node.
   */
  final static Long MASTER = 0L;

  /**
   * Atomic counter to generate unique identifiers for read-replicas.
   */
  final @NotNull AtomicLong nextConfigId = new AtomicLong(1L);

  /**
   * Returns the configuration of the master node.
   */
  final @NotNull PsqlInstanceConfig masterConfig() {
    final PsqlInstanceConfig masterConfig = configs.get(MASTER);
    assert masterConfig != null;
    return masterConfig;
  }

  /**
   * A map of all available server configurations.
   */
  final @NotNull ConcurrentHashMap<Long, @NotNull PsqlInstanceConfig> configs;

  @Override
  public Connection getConnection() throws SQLException {
    return new PostgresConnectionReference();
  }

  public Connection getConnection(boolean readOnly) throws SQLException {
    return new PostgresConnectionReference();
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    // TODO: We should simply create a new config builder from the existing configuration (TBD).
    //       Then we can change the user and password and build a new config.
    //       Eventually, we could create a new PostgresConnectionHandle with a connection is generally not pooled.
    //       Even while this is sub-optimal, it would work, maybe better than throwing an exception?
    throw new SQLFeatureNotSupportedException();
  }

  final @NotNull AtomicReference<@NotNull PrintWriter> logWriter;

  @Override
  public PrintWriter getLogWriter() {
    return logWriter.get();
  }

  @Override
  public void setLogWriter(PrintWriter out) {
    if (out == null) {
      out = slf4jLogWriter;
    }
    logWriter.set(out);
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public int getLoginTimeout() {
    return Math.min(1, (int) TimeUnit.MILLISECONDS.toSeconds(masterConfig().connTimeout));
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new SQLException();
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) {
    return false;
  }

  @Override
  protected void destruct() {
  }
}
