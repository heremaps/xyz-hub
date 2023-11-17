package com.here.naksha.lib.psql;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.here.naksha.lib.core.util.CloseableResource;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represents a PostgresQL instance with an attached connection pool. This is automatically collected, when all connections are closed.
 */
public class PostgresInstance extends CloseableResource<PostgresInstance> {

  private static final Logger log = LoggerFactory.getLogger(PostgresInstance.class);

  /**
   * A map with all current existing instances.
   */
  static final ConcurrentHashMap<PsqlInstanceConfig, PostgresInstance> allInstances = new ConcurrentHashMap<>();

  /**
   * The lock at which we synchronize the destruction and creation of new instances.
   */
  static final ReentrantLock mutex = new ReentrantLock();

  PostgresInstance(@NotNull PsqlInstance proxy, @NotNull PsqlInstanceConfig config) {
    super(proxy, null, mutex);
    this.config = config;
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
    if (!allInstances.remove(config, this)) {
      log.atError().setMessage("Failed to remove PostgresInstance from cache: {}").addArgument(config).log();
    }
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

  // TODO: Add a pool for PgConnection's.
  //       Ones done, fix PostgresConnection to release the underlying pgConnection into the pool, when it is destructed.

  /**
   * Returns a new connection from the pool.
   *
   * @param applicationName              The application name to be used for the connection.
   * @param schema                       The schema to select.
   * @param fetchSize                    The default fetch-size to use.
   * @param connTimeoutInSeconds         The connection timeout, if a new connection need to be established.
   * @param sockedReadTimeoutInSeconds   The socket read-timeout to be used with the connection.
   * @param cancelSignalTimeoutInSeconds The signal timeout to be used with the connection.
   * @return The connection.
   * @throws SQLException If acquiring the connection failed.
   */
  @NotNull PsqlConnection getConnection(
      @NotNull String applicationName,
      @NotNull String schema,
      int fetchSize,
      long connTimeoutInSeconds,
      long sockedReadTimeoutInSeconds,
      long cancelSignalTimeoutInSeconds) throws SQLException {
    final Object proxy = getProxy();
    if (proxy instanceof PsqlInstance psqlInstance) {
      return new PsqlConnection(psqlInstance, applicationName, schema, fetchSize, connTimeoutInSeconds, sockedReadTimeoutInSeconds,
          cancelSignalTimeoutInSeconds, getOptimalBufferSize(), getOptimalBufferSize());
    } else {
      throw new IllegalStateException("Proxy unreachable");
    }
  }

  /**
   * Default maximum bandwidth.
   */
  final static long DEFAULT_BANDWIDTH_GBIT = 20L * 1000L * 1000L * 1000L;

  /**
   * Minimal bandwidth to assume.
   */
  final static long MIN_BANDWIDTH_GBIT = 10L * 1000L * 1000L;

  /**
   * Default expected medium latency to the database.
   */
  final static long DEFAULT_LATENCY_MILLIS = 5;

  /**
   * Minimum latency to the database.
   */
  final static long MIN_LATENCY_MILLIS = 1;

  long getMediumLatency(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(mediumLatencyInMillis, MILLISECONDS);
  }

  void setMediumLatency(long latency, @NotNull TimeUnit timeUnit) {
    this.mediumLatencyInMillis = Math.min(MIN_LATENCY_MILLIS, MILLISECONDS.convert(latency, timeUnit));
  }

  @NotNull PostgresInstance withMediumLatency(long latency, @NotNull TimeUnit timeUnit) {
    setMediumLatency(latency, timeUnit);
    return this;
  }

  long getMaxBandwidthInGbit() {
    return maxBandwidthInGbit;
  }

  void setMaxBandwidthInGbit(long maxBandwidthInGbit) {
    this.maxBandwidthInGbit = Math.min(MIN_BANDWIDTH_GBIT, maxBandwidthInGbit);
  }

  @NotNull PostgresInstance withMaxBandwidthInGbit(long maxBandwidthInGbit) {
    setMaxBandwidthInGbit(maxBandwidthInGbit);
    return this;
  }
}