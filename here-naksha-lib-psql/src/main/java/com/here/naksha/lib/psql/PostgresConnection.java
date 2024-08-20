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
package com.here.naksha.lib.psql;

import static java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT;

import com.here.naksha.lib.core.util.CloseableResource;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.PGProperty;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.HostSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A managed PostgresQL connection that supports pooling. When the connection is {@link #destruct() destructed}, it will create a new
 * connection wrapper and add it into the connection pool of the {@link PostgresInstance} it belongs to.
 */
final class PostgresConnection extends CloseableResource<PostgresInstance> {

  private static final Logger log = LoggerFactory.getLogger(PostgresConnection.class);
  private static final AtomicLong nextId = new AtomicLong();

  /**
   * Wrap a given existing postgres connection.
   *
   * @param proxy            The proxy that wraps the connection.
   * @param postgresInstance The instance to which this connection belongs.
   * @param pg_connection    The connection to wrap.
   * @throws SQLException If any error occurred while wrapping the connection.
   */
  PostgresConnection(
      @NotNull PsqlConnection proxy,
      @NotNull PostgresInstance postgresInstance,
      @NotNull PgConnection pg_connection)
      throws SQLException {
    super(proxy, postgresInstance);
    this.postgresInstance = postgresInstance;
    this.instanceUrl = postgresInstance.config.toString();
    this.id = nextId.getAndIncrement();
    final PsqlInstanceConfig config = postgresInstance.config;

    pgConnection = pg_connection;
    pgConnection.setAutoCommit(false);
    pgConnection.setReadOnly(config.readOnly);
    pgConnection.setHoldability(CLOSE_CURSORS_AT_COMMIT);
    log.atDebug()
        .setMessage("Open connection {} to instance {}")
        .addArgument(id)
        .addArgument(instanceUrl)
        .log();
  }

  /**
   * Creates a new connection.
   *
   * @param proxy                       The proxy that wraps the connection.
   * @param postgresInstance            The instance to which this connection belongs.
   * @param connTimeoutInMillis         The connection timeout in milliseconds.
   * @param sockedReadTimeoutInMillis   The socket-read timeout in milliseconds.
   * @param cancelSignalTimeoutInMillis The
   * @param receiveBufferSize           The receive-buffer size in byte.
   * @param sendBufferSize              The send-buffer size in byte.
   * @throws SQLException If establishing the connection failed.
   */
  PostgresConnection(
      @NotNull PsqlConnection proxy,
      @NotNull PostgresInstance postgresInstance,
      long connTimeoutInMillis,
      long sockedReadTimeoutInMillis,
      long cancelSignalTimeoutInMillis,
      long receiveBufferSize,
      long sendBufferSize)
      throws SQLException {
    super(proxy, postgresInstance);
    this.postgresInstance = postgresInstance;
    this.instanceUrl = postgresInstance.config.toString();
    this.id = nextId.getAndIncrement();
    final PsqlInstanceConfig config = postgresInstance.config;

    final Properties props = new Properties();
    props.setProperty(PGProperty.PG_DBNAME.getName(), config.db);
    props.setProperty(PGProperty.USER.getName(), config.user);
    props.setProperty(PGProperty.PASSWORD.getName(), config.password);
    props.setProperty(PGProperty.BINARY_TRANSFER.getName(), "true");
    if (config.readOnly) {
      props.setProperty(PGProperty.READ_ONLY.getName(), "true");
    }
    // TODO : can be changed to debug later, when timeout issues have settled (and logs are too noisy)
    log.info(
        "Init connection using connectTimeout={}ms, socketTimeout={}ms, cancelSignalTimeout={}ms",
        connTimeoutInMillis,
        sockedReadTimeoutInMillis,
        cancelSignalTimeoutInMillis);
    props.setProperty(
        PGProperty.CONNECT_TIMEOUT.getName(),
        Long.toString(Math.min(Integer.MAX_VALUE, connTimeoutInMillis / 1000L)));
    props.setProperty(
        PGProperty.SOCKET_TIMEOUT.getName(),
        Long.toString(Math.min(Integer.MAX_VALUE, sockedReadTimeoutInMillis / 1000L)));
    props.setProperty(
        PGProperty.CANCEL_SIGNAL_TIMEOUT.getName(),
        Long.toString(Math.min(Integer.MAX_VALUE, cancelSignalTimeoutInMillis / 1000L)));
    props.setProperty(PGProperty.RECEIVE_BUFFER_SIZE.getName(), Long.toString(receiveBufferSize));
    props.setProperty(PGProperty.SEND_BUFFER_SIZE.getName(), Long.toString(sendBufferSize));
    props.setProperty(PGProperty.REWRITE_BATCHED_INSERTS.getName(), "true");
    props.setProperty(PGProperty.LOG_UNCLOSED_CONNECTIONS.getName(), "true");

    pgConnection = new PgConnection(new HostSpec[] {config.hostSpec}, props, config.url);
    pgConnection.setAutoCommit(false);
    pgConnection.setReadOnly(config.readOnly);
    pgConnection.setHoldability(CLOSE_CURSORS_AT_COMMIT);
    log.atDebug()
        .setMessage("Open connection {} to instance {}")
        .addArgument(id)
        .addArgument(instanceUrl)
        .log();
  }

  final @NotNull Long id;
  private final WeakReference<PostgresConnection> weakRef = new WeakReference<>(this);
  private @Nullable PgConnection pgConnection;

  @Nullable
  PostgresInstance postgresInstance;

  long autoCloseAtEpoch;
  final @NotNull String instanceUrl;

  /**
   * Returns the {@link PsqlConnection} proxy.
   *
   * @return the {@link PsqlConnection} proxy; {@code null}, if the proxy was already garbage collected.
   */
  public @Nullable PsqlConnection getPsqlConnection() {
    final Object proxy = getProxy();
    if (proxy instanceof PsqlConnection) {
      return (PsqlConnection) proxy;
    }
    return null;
  }

  /**
   * Set the default fetch-size.
   *
   * @param fetchSize The new default-fetch size.
   * @return this.
   * @throws SQLException If setting the new size failed.
   */
  @NotNull
  PostgresConnection withFetchSize(int fetchSize) throws SQLException {
    final PgConnection pgConnection = get();
    pgConnection.setDefaultFetchSize(fetchSize);
    return this;
  }

  /**
   * Set the read-timeout on the underlying socket.
   *
   * @param timeout  The timeout.
   * @param timeUnit The time-unit in which the timeout is provided.
   * @return this.
   * @throws SQLException If setting the timeout failed.
   */
  @NotNull
  PostgresConnection withSocketReadTimeout(long timeout, TimeUnit timeUnit) throws SQLException {
    final PgConnection pgConnection = get();
    long millis = TimeUnit.MILLISECONDS.convert(timeout, timeUnit);
    if (millis > Integer.MAX_VALUE) {
      millis = Integer.MAX_VALUE;
    }
    try {
      pgConnection.getQueryExecutor().setNetworkTimeout((int) millis);
    } catch (IOException e) {
      throw new SQLException(e.getMessage(), EPsqlState.CONNECTION_FAILURE.toString(), e);
    }
    return this;
  }

  @Override
  protected @NotNull PostgresInstance parent() {
    final PostgresInstance parent = super.parent();
    assert parent != null;
    return parent;
  }

  /**
   * Returns the underlying PgConnection.
   *
   * @return the underlying PgConnection.
   * @throws SQLException If the connection is closed ({@link EPsqlState#CONNECTION_DOES_NOT_EXIST}).
   */
  @NotNull
  PgConnection get() throws SQLException {
    final PgConnection conn = pgConnection;
    if (conn == null || isClosed()) {
      throw new SQLException("Connection closed", EPsqlState.CONNECTION_DOES_NOT_EXIST.toString());
    }
    return conn;
  }

  @Override
  protected boolean mayAutoClose(long now) {
    final PostgresInstance postgresInstance = this.postgresInstance;
    final PsqlConnection psqlConnection = getPsqlConnection();
    final long autoCloseAtEpoch = this.autoCloseAtEpoch;
    return psqlConnection != null && postgresInstance != null && autoCloseAtEpoch > 0 && autoCloseAtEpoch < now;
  }

  @Override
  protected boolean tryAutoClose(long now) {
    final PostgresInstance postgresInstance = this.postgresInstance;
    final PsqlConnection psqlConnection = getPsqlConnection();
    final long autoCloseAtEpoch = this.autoCloseAtEpoch;
    if (psqlConnection != null && postgresInstance != null && autoCloseAtEpoch > 0 && autoCloseAtEpoch < now) {
      // We are timed-out, remove our self from the connection pool.
      // If we removed our self from the connection pool successfully, then we are closed and ready for
      // destruction.
      // The destructor will be called after returning.
      if (postgresInstance.connectionPool.remove(psqlConnection, psqlConnection)) {
        log.atDebug()
            .setMessage("Remove connection {} from idle pool of instance {}")
            .addArgument(id)
            .addArgument(instanceUrl)
            .log();
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("resource")
  @Override
  protected void destruct() {
    // Note: We only enter this method ones in the live time.
    assert this.pgConnection != null;
    assert this.postgresInstance != null;
    try {
      try {
        // Rollback pending changes.
        this.pgConnection.rollback();

        // If this connection is not auto-closable, which means, it was not auto-closed and
        // there is free space in the idle connections pool, then we add this connection into
        // the idle pool.
        // Note: We always create a new connection wrapper to avoid that there is any pending
        // reference to the connection. This could happen, if the user invoked "close()", but
        // still keeps a reference to the connection somewhere.
        if (!this.isAutoClosable()
            && this.postgresInstance.connectionPool.size() < this.postgresInstance.maxPoolSize) {
          final PsqlConnection psqlConnection = new PsqlConnection(this.postgresInstance, this.pgConnection);
          psqlConnection.postgresConnection.autoCloseAtEpoch =
              System.currentTimeMillis() + this.postgresInstance.idleTimeoutInMillis;
          psqlConnection.postgresConnection.setAutoClosable(true);
          this.postgresInstance.connectionPool.put(psqlConnection, psqlConnection);
          return;
        }
      } catch (Exception e) {
        log.atInfo()
            .setMessage("Failed to rollback connection {} to instance {}")
            .addArgument(id)
            .addArgument(instanceUrl)
            .setCause(e)
            .log();
      } finally {
        this.postgresInstance = null;
      }
      // If we reach this point, the connection is not placed into the idle connections pool, but eventually
      // closed.
      log.atInfo()
          .setMessage("Close connection {} to {}")
          .addArgument(id)
          .addArgument(instanceUrl)
          .log();
      this.pgConnection.close();
    } catch (Exception e) {
      log.atInfo()
          .setMessage("Failed to close connection {} to instance {}")
          .addArgument(id)
          .addArgument(instanceUrl)
          .setCause(e)
          .log();
    } finally {
      // Should there be still a reference to this connection, at least it
      // does not have any reference to the underlying pgConnection anymore!
      this.pgConnection = null;
    }
  }

  @Override
  public @NotNull String toString() {
    return instanceUrl;
  }
}
