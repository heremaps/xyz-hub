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

package com.here.xyz.util.db.datasource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CachedPooledDataSources extends PooledDataSources {

  private static final Logger logger = LogManager.getLogger();

  private static final Map<String, Map<String, StaticDataSources>> cache = new ConcurrentHashMap<>();

  public CachedPooledDataSources(DatabaseSettings dbSettings) {
    super(dbSettings);
  }

  @Override
  public DataSource getReader() {
    return getCachedDataSources().getReader();
  }

  @Override
  public DataSource getWriter() {
    return getCachedDataSources().getWriter();
  }

  public static void invalidateCache() {
    Map<String, Map<String, StaticDataSources>> oldCache = new HashMap<>(cache);
    logger.info("Clearing data sources cache. Current cache size: {}, Closing old data sources ...", cache.size());
    cache.clear();
    oldCache.forEach((settingsId, cacheEntry) -> cacheEntry.forEach((cacheKey, dataSources) -> closeDataSources(settingsId, dataSources)));
  }

  private static void closeDataSources(String settingsId, StaticDataSources dataSources) {
    try {
      logger.info("Closing data sources with ID {}. Current cache size: {}", settingsId, cache.size());
      dataSources.close();
    }
    catch (Exception e) {
      logger.warn("Error closing obsolete cached data sources for {}. Current cache size: {}", settingsId, e, cache.size());
    }
  }

  private StaticDataSources getCachedDataSources() {
    Map<String, StaticDataSources> cacheEntry = cache.get(dbSettings.getId());
    String cacheKey = dbSettings.getCacheKey();
    if (cacheEntry == null || cacheEntry.get(cacheKey) == null)
      Optional.ofNullable(cache.put(dbSettings.getId(), cacheEntry = createDataSources(dbSettings.getId(), cacheKey)))
          .ifPresent(oldCacheEntry -> oldCacheEntry.forEach((k, oldDataSources) -> closeDataSources(dbSettings.getId(), oldDataSources)));

    return cacheEntry.get(cacheKey);
  }

  private Map<String, StaticDataSources> createDataSources(String settingsId, String cacheKey) {
    logger.info("Creating new data sources with ID {} and cache key {}. Current cache size: {}", settingsId, cacheKey, cache.size());
    return Collections.singletonMap(cacheKey, new StaticDataSources(super.getReader(), super.getWriter()));
  }

  @Override
  public void close() throws Exception {
    invalidateCache();
  }
}
