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

import static com.here.naksha.lib.core.NakshaContext.currentLogger;

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

/** A connection pool with static caching. */
public final class PsqlPool implements AutoCloseable {

  private static final String PSQL_CLEANER_NAME = "PsqlPoolCleaner";

  @Override
  public void close() {
    // This is just for the user to be able to use the pool in a try to keep our soft-reference
    // alive.
  }

  private static class PsqlConnPoolRef extends SoftReference<PsqlPool> {

    public PsqlConnPoolRef(@NotNull PsqlPool referent) {
      super(referent);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(PsqlPool.class);

  private static final AtomicReference<Thread> cacheCleaner = new AtomicReference<>();

  private static final ConcurrentHashMap<@NotNull PsqlPoolConfig, @NotNull PsqlConnPoolRef> cache =
      new ConcurrentHashMap<>();

  private PsqlPool(@NotNull HikariDataSource dataSource, @NotNull PsqlPoolConfig config) {
    this.config = config;
    this.dataSource = dataSource;
    Thread cleaner = cacheCleaner.get();
    if (cleaner == null) {
      cleaner = new Thread(PsqlPool::clearCache, "PsqlConnectionPoolCleaner");
      if (cacheCleaner.compareAndSet(null, cleaner)) {
        // Only one thread ones in the live-time of the application will enter this!
        cleaner.start();
      }
    }
  }

  @JsonIgnore
  public final @NotNull PsqlPoolConfig config;

  @JsonIgnore
  public final @NotNull HikariDataSource dataSource;

  private static void clearCache() {
    final Enumeration<@NotNull PsqlPoolConfig> keyEnum = cache.keys();
    while (keyEnum.hasMoreElements()) {
      final PsqlPoolConfig config = keyEnum.nextElement();
      final PsqlConnPoolRef poolRef = cache.get(config);
      final PsqlPool pool = poolRef.get();
      if (pool == null) {
        Thread.currentThread().getUncaughtExceptionHandler();
        cache.remove(config, poolRef);
        currentLogger().info("Remove garbage collected connection pool {}", config);
      }
    }
  }

  /**
   * Returns the connection pool for the given configuration. If no such pool exists yet, create a
   * new one.
   *
   * @param config the configuration.
   * @return the pool.
   */
  public static @NotNull PsqlPool get(final @NotNull PsqlPoolConfig config) {
    PsqlConnPoolRef poolRef;
    PsqlPool pool;
    do {
      poolRef = cache.get(config);
      if (poolRef == null) {
        return createAndCacheDataSource(config);
      }
      pool = poolRef.get();
    } while (pool == null);
    return pool;
  }

  private static synchronized @NotNull PsqlPool createAndCacheDataSource(final @NotNull PsqlPoolConfig config) {
    PsqlConnPoolRef poolRef;
    PsqlPool pool;
    poolRef = cache.get(config);
    pool = poolRef != null ? poolRef.get() : null;
    if (pool != null) {
      return pool;
    }

    final HikariConfig hikariConfig = new HikariConfig();
    hikariConfig.setDriverClassName(config.driverClass);
    hikariConfig.setJdbcUrl(config.url);
    hikariConfig.setUsername(config.user);
    hikariConfig.setPassword(config.password);
    hikariConfig.setConnectionTimeout(config.connTimeout);
    hikariConfig.setMinimumIdle(config.minPoolSize);
    hikariConfig.setMaximumPoolSize(config.maxPoolSize);
    hikariConfig.setIdleTimeout(config.idleTimeout);
    hikariConfig.setAutoCommit(false);
    final HikariDataSource hikariDataSource = new HikariDataSource(hikariConfig);
    pool = new PsqlPool(hikariDataSource, config);
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
