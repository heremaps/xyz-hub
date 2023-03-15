package com.here.mapcreator.ext.naksha;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.ref.SoftReference;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connection pool with static caching.
 */
public final class NPsqlPool implements AutoCloseable {

  private static final String PSQL_CLEANER_NAME = "PsqlPoolCleaner";

  @Override
  public void close() {
    // This is just for the user to be able to use the pool in a try to keep our soft-reference
    // alive.
  }

  private static class PsqlConnPoolRef extends SoftReference<NPsqlPool> {

    public PsqlConnPoolRef(@NotNull NPsqlPool referent) {
      super(referent);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(NPsqlPool.class);

  private static final AtomicReference<Thread> cacheCleaner = new AtomicReference<>();

  private static final ConcurrentHashMap<@NotNull NPsqlPoolConfig, @NotNull PsqlConnPoolRef> cache =
      new ConcurrentHashMap<>();

  private NPsqlPool(@NotNull HikariDataSource dataSource, @NotNull NPsqlPoolConfig config) {
    this.config = config;
    this.dataSource = dataSource;
    Thread cleaner = cacheCleaner.get();
    if (cleaner == null) {
      cleaner = new Thread(NPsqlPool::clearCache, "PsqlConnectionPoolCleaner");
      if (cacheCleaner.compareAndSet(null, cleaner)) {
        // Only one thread ones in the live-time of the application will enter this!
        cleaner.start();
      }
    }
  }

  @JsonIgnore
  public final @NotNull NPsqlPoolConfig config;

  @JsonIgnore
  public final @NotNull HikariDataSource dataSource;

  private static void clearCache() {
    final Enumeration<@NotNull NPsqlPoolConfig> keyEnum = cache.keys();
    while (keyEnum.hasMoreElements()) {
      final NPsqlPoolConfig config = keyEnum.nextElement();
      final PsqlConnPoolRef poolRef = cache.get(config);
      final NPsqlPool pool = poolRef.get();
      if (pool == null) {
        cache.remove(config, poolRef);
        logger.info(
            "{} - Remove garbage collected connection pool {}",
            Thread.currentThread().getName(),
            config);
      }
    }
  }

  /**
   * Returns the connection pool for the given configuration. If no such pool exists yet, create a new one.
   *
   * @param config the configuration.
   * @return the pool.
   */
  public static @NotNull NPsqlPool get(final @NotNull NPsqlPoolConfig config) {
    PsqlConnPoolRef poolRef;
    NPsqlPool pool;
    do {
      poolRef = cache.get(config);
      if (poolRef == null) {
        return createAndCacheDataSource(config);
      }
      pool = poolRef.get();
    } while (pool == null);
    return pool;
  }

  private static synchronized @NotNull NPsqlPool createAndCacheDataSource(final @NotNull NPsqlPoolConfig config) {
    PsqlConnPoolRef poolRef;
    NPsqlPool pool;
    poolRef = cache.get(config);
    pool = poolRef != null ? poolRef.get() : null;
    if (pool != null) {
      return pool;
    }

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDriverClassName(config.driverClass);
    hikariConfig.setJdbcUrl(config.url);
    hikariConfig.setUsername(config.url);
    hikariConfig.setPassword(config.password);
    hikariConfig.setConnectionTimeout(config.connTimeout);
    hikariConfig.setMinimumIdle(config.minPoolSize);
    hikariConfig.setMaximumPoolSize(config.maxPoolSize);
    hikariConfig.setIdleTimeout(config.idleTimeout);
    hikariConfig.setAutoCommit(false);
    final HikariDataSource hikariDataSource = new HikariDataSource(hikariConfig);
    pool = new NPsqlPool(hikariDataSource, config);
    poolRef = new PsqlConnPoolRef(pool);
    cache.put(config, poolRef);
    return pool;
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void finalize() {
    logger.info("{} - Garbage collect connection pool {}", PSQL_CLEANER_NAME, config);
    dataSource.close();
  }
}
