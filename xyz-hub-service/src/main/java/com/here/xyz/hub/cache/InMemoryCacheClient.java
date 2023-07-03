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

package com.here.xyz.hub.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.Weigher;
import com.here.xyz.hub.Service;
import io.vertx.core.Future;

public class InMemoryCacheClient implements CacheClient {

  private Cache<String, byte[]> cache = CacheBuilder
      .newBuilder()
      .maximumWeight(Service.configuration.CACHE_SIZE_MB * 1024 * 1024)
      .weigher((Weigher<String, byte[]>) (key, value) -> value.length)
      .build();

  private static InMemoryCacheClient instance;

  private InMemoryCacheClient() {}

  public static synchronized CacheClient getInstance() {
    if (instance == null) instance = new InMemoryCacheClient();
    return instance;
  }

  @Override
  public Future<byte[]> get(String key) {
    return Future.succeededFuture(cache.getIfPresent(key));
  }

  @Override
  public void set(String key, byte[] value, long ttl) {
    //NOTE: ttl is ignored - Values will get evicted when the cache reaches its size limit on an LRU basis
    cache.put(key, value);
  }

  @Override
  public void remove(String key) {
    cache.invalidate(key);
  }

  @Override
  public void shutdown() {
    //Nothing to do
  }
}
