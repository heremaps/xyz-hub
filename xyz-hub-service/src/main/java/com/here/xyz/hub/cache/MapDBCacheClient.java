/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.hub.cache;

import com.here.xyz.hub.Service;
import io.vertx.core.Handler;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;

public class MapDBCacheClient implements CacheClient {

  private static MapDBCacheClient client;

  public static synchronized MapDBCacheClient get() {
    if (client == null) {
      client = new MapDBCacheClient();
    }

    return client;
  }

  private MapDBCacheClient() {
    executor = Executors.newScheduledThreadPool(2);
    db = DBMaker.memoryDirectDB().make();
    map = db.hashMap("cache")
        .keySerializer(Serializer.STRING)
        .valueSerializer(Serializer.BYTE_ARRAY)
        .expireStoreSize(Service.configuration.OFF_HEAP_CACHE_SIZE_MB * 1024 * 1024)
        .expireAfterCreate(3, TimeUnit.MINUTES)
        .expireAfterUpdate(3, TimeUnit.MINUTES)
        .expireAfterGet(3, TimeUnit.MINUTES)
        .expireExecutor(executor)
        .expireExecutorPeriod(5_000)
        .create();
  }

  private final ScheduledExecutorService executor;
  private final HTreeMap<String, byte[]> map;
  private final DB db;

  @Override
  public void get(String key, Handler<byte[]> handler) {
    byte[] result = map.get(key);
    handler.handle(result);
  }

  @Override
  public void set(String key, byte[] value, long ttl) {
    map.put(key, value);
  }

  @Override
  public void remove(String key) {
    map.remove(key);
  }

  @Override
  public void shutdown() {
    map.close();
    executor.shutdown();
  }
}