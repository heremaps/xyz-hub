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

import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import io.vertx.core.Future;
import java.nio.ByteBuffer;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

public class OHCacheClient implements CacheClient {

  private static final Logger logger = LogManager.getLogger();
  private static OHCacheClient client;
  private static AtomicInteger clientCount = new AtomicInteger();

  private final ScheduledExecutorService executors;
  private OHCache<byte[], byte[]> cache;

  static synchronized OHCacheClient getInstance() {
    if (client == null) client = new OHCacheClient();
    return client;
  }

  private OHCacheClient() {
    executors = new ScheduledThreadPoolExecutor(2, Core.newThreadFactory("ohCache" + clientCount.getAndIncrement()));
    cache = createCache(Service.configuration.OFF_HEAP_CACHE_SIZE_MB, executors, false);
  }

  @Override
  public Future<byte[]> get(String key) {
    try {
      return Future.succeededFuture(cache.get(key.getBytes()));
    }
    catch (Throwable e) {
      logger.warn("Error when trying to read key " + key + " from OH-cache", e);
      return Future.succeededFuture(null);
    }
  }

  @Override
  public void set(String key, byte[] value, long ttl) {
    cache.put(key.getBytes(), value);
  }

  @Override
  public void remove(String key) {
    cache.remove(key.getBytes());
  }

  @Override
  public void shutdown() {
    executors.shutdown();
  }

  /**
   * Creates an OHCache with the given size in MB
   * @param size The size of the cache in MB
   * @return The Cache instance
   */
  public static OHCache<byte[], byte[]> createCache(int size, ScheduledExecutorService executors, boolean withTimeouts) {
    CacheSerializer<byte[]> serializer = new Serializer();
    OHCacheBuilder<byte[], byte[]> builder = OHCacheBuilder.newBuilder();
    return builder.capacity(size * 1024L * 1024L)
        .eviction(Eviction.W_TINY_LFU)
        .keySerializer(serializer)
        .valueSerializer(serializer)
        .executorService(executors)
        .timeouts(withTimeouts)
        .build();
  }

  private static class Serializer implements CacheSerializer<byte[]> {

    @Override
    public void serialize(byte[] bytes, ByteBuffer byteBuffer) {
      byteBuffer.put(bytes);
    }

    @Override
    public byte[] deserialize(ByteBuffer byteBuffer) {
      int size = byteBuffer.remaining();
      byte[] result = new byte[size];
      byteBuffer.get(result, 0, size);
      return result;
    }

    @Override
    public int serializedSize(byte[] bytes) {
      return bytes.length;
    }
  }
}