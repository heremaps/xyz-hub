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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.here.naksha.lib.core.util.ClosableRootResource;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a PostgresQL instance with an attached connection pool. This is automatically collected, when all connections are closed.
 */
public class PostgresInstance extends ClosableRootResource {

  private static final Logger log = LoggerFactory.getLogger(PostgresInstance.class);

  static final ConcurrentHashMap<PsqlInstanceConfig, PostgresInstance> allInstances = new ConcurrentHashMap<>();

  /**
   * The lock at which we synchronize the destruction and creation of new instances.
   */
  static final ReentrantLock mutex = new ReentrantLock();

  /**
   * A map with all connections to this instance that are currently idle. Connections add them-self into this pool when being destructed and
   * the pool has space left.
   */
  final @NotNull ConcurrentHashMap<PsqlConnection, PsqlConnection> connectionPool;

  /**
   * The timeout when to close idle connections.
   */
  long idleTimeoutInMillis;

  /**
   * The maximum amount of connections to keep in the pool.
   */
  int maxPoolSize;

  PostgresInstance(@NotNull PsqlInstance proxy, @NotNull PsqlInstanceConfig config) {
    super(proxy, mutex);
    this.config = config;
    this.connectionPool = new ConcurrentHashMap<>();
    this.idleTimeoutInMillis = TimeUnit.MINUTES.toMillis(2);
    this.maxPoolSize = 100;
    allInstances.put(config, this);
  }

  /**
   * Returns the {@link PsqlInstance} proxy.
   *
   * @return the {@link PsqlInstance} proxy; {@code null}, if the proxy was already garbage collected.
   */
  public @Nullable PsqlInstance getPsqlInstance() {
    final Object proxy = getProxy();
    if (proxy instanceof PsqlInstance) {
      return (PsqlInstance) proxy;
    }
    return null;
  }

  @Override
  protected boolean logLeak() {
    return false;
  }

  /**
   * The instance configuration.
   */
  final @NotNull PsqlInstanceConfig config;

  @Override
  protected void destruct() {
    allInstances.remove(config, this);
  }

  /**
   * The maximum bandwidth for connections for the database.
   */
  long maxBandwidthInGbit = DEFAULT_BANDWIDTH_GBIT;

  /**
   * The medium latency for connections to the database.
   */
  long mediumLatencyInMillis = DEFAULT_LATENCY_MILLIS;

  /**
   * Returns the optimal buffer-size for connections in byte.
   *
   * @return the optimal buffer-size for connections in byte.
   */
  final long getOptimalBufferSize() {
    // We want to be able to send with a max speed, therefore:
    // Throughput (in bytes/second) = Buffer Size (in bytes) / Latency (in seconds)
    // Buffer Size (in bytes/second) = Throughput (in bytes) / Latency (in seconds)
    double opt = (maxBandwidthInGbit / 8d) / (mediumLatencyInMillis / 1000d);
    return Math.min(16384, Math.round(opt));
  }

  /**
   * Returns a connection from the connection pool or creates a new connection.
   *
   * @param connTimeoutInMillis         The connection timeout, if a new connection need to be established.
   * @param sockedReadTimeoutInMillis   The socket read-timeout to be used with the connection.
   * @param cancelSignalTimeoutInMillis The signal timeout to be used with the connection.
   * @return The connection.
   * @throws SQLException If acquiring the connection failed.
   */
  @NotNull
  PsqlConnection getConnection(
      long connTimeoutInMillis, long sockedReadTimeoutInMillis, long cancelSignalTimeoutInMillis)
      throws SQLException {
    // Try to reuse an existing pooled idle connection.
    final ConcurrentHashMap<PsqlConnection, PsqlConnection> connectionPool = this.connectionPool;
    final Enumeration<PsqlConnection> keyEnum = connectionPool.keys();
    while (keyEnum.hasMoreElements()) {
      try {
        final PsqlConnection psqlConnection = keyEnum.nextElement();
        if (connectionPool.remove(psqlConnection, psqlConnection)) {
          final PostgresConnection postgresConnection = psqlConnection.postgresConnection;
          postgresConnection.setAutoClosable(false);
          postgresConnection.autoCloseAtEpoch = 0L;
          if (!psqlConnection.isClosed()) {
            log.atDebug()
                .setMessage("Reuse idle connection {} of instance {}")
                .addArgument(postgresConnection.id)
                .addArgument(postgresConnection.instanceUrl)
                .log();
            postgresConnection.withSocketReadTimeout(sockedReadTimeoutInMillis, MILLISECONDS);
            return psqlConnection;
          }
        }
      } catch (NoSuchElementException ignore) {
      }
    }
    // No idle connection found, create a new one.
    return new PsqlConnection(
        this,
        connTimeoutInMillis,
        sockedReadTimeoutInMillis,
        cancelSignalTimeoutInMillis,
        getOptimalBufferSize(),
        getOptimalBufferSize());
  }

  /**
   * Default maximum bandwidth.
   */
  static final long DEFAULT_BANDWIDTH_GBIT = 20L * 1000L * 1000L * 1000L;

  /**
   * Minimal bandwidth to assume.
   */
  static final long MIN_BANDWIDTH_GBIT = 10L * 1000L * 1000L;

  /**
   * Default expected medium latency to the database.
   */
  static final long DEFAULT_LATENCY_MILLIS = 5;

  /**
   * Minimum latency to the database.
   */
  static final long MIN_LATENCY_MILLIS = 1;

  long getMediumLatency(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(mediumLatencyInMillis, MILLISECONDS);
  }

  void setMediumLatency(long latency, @NotNull TimeUnit timeUnit) {
    this.mediumLatencyInMillis = Math.min(MIN_LATENCY_MILLIS, MILLISECONDS.convert(latency, timeUnit));
  }

  @NotNull
  PostgresInstance withMediumLatency(long latency, @NotNull TimeUnit timeUnit) {
    setMediumLatency(latency, timeUnit);
    return this;
  }

  long getMaxBandwidthInGbit() {
    return maxBandwidthInGbit;
  }

  void setMaxBandwidthInGbit(long maxBandwidthInGbit) {
    this.maxBandwidthInGbit = Math.min(MIN_BANDWIDTH_GBIT, maxBandwidthInGbit);
  }

  @NotNull
  PostgresInstance withMaxBandwidthInGbit(long maxBandwidthInGbit) {
    setMaxBandwidthInGbit(maxBandwidthInGbit);
    return this;
  }

  @Override
  public void logStats(Logger log) {
    super.logStats(log);
    log.info(
        "[Instance connectionPool stats => instance,count] - InstanceConnectionPoolCount {} {}",
        this,
        connectionPool.size());
  }
}
