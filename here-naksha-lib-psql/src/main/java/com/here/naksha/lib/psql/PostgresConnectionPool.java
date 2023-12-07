package com.here.naksha.lib.psql;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;

/**
 * The connection pool, connections are added as weak-references, when the client closes the connection.
 */
final class PostgresConnectionPool extends ConcurrentHashMap<WeakReference<PostgresConnection>, Boolean> {

  /**
   * Creates a new connection pool.
   * @param config The configuration for which to create it.
   */
  PostgresConnectionPool(@NotNull PsqlInstanceConfig config) {
    this.config = config;
    this.idleTimeoutInMillis = TimeUnit.MINUTES.toMillis(2);
    this.maxSize = 128;
  }

  /**
   * The configuration of the connection pool.
   */
  final @NotNull PsqlInstanceConfig config;

  /**
   * The timeout when to close idle connections.
   */
  long idleTimeoutInMillis;

  /**
   * The maximum amount of connections
   */
  int maxSize;
}