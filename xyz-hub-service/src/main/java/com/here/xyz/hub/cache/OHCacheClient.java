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
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import org.caffinitas.ohc.CacheSerializer;
import org.caffinitas.ohc.Eviction;
import org.caffinitas.ohc.OHCache;
import org.caffinitas.ohc.OHCacheBuilder;

public class OHCacheClient implements CacheClient {

  private static OHCacheClient client;
  private static OHCache<byte[], byte[]> cache;

  public static synchronized OHCacheClient getInstance() {
    if (client == null) {
      client = new OHCacheClient();
    }

    return client;
  }

  private OHCacheClient() {
    CacheSerializer<byte[]> serializer = new Serializer();
    executor = Executors.newScheduledThreadPool(2);
        OHCacheBuilder<byte[], byte[]> builder = OHCacheBuilder.newBuilder();
        cache = builder.capacity(Service.configuration.OFF_HEAP_CACHE_SIZE_MB * 1024 * 1024)
        .eviction(Eviction.W_TINY_LFU)
        .keySerializer(serializer)
        .valueSerializer(serializer)
        .executorService(executor)
        .build();
  }

  private final ScheduledExecutorService executor;

  @Override
  public void get(String key, Handler<byte[]> handler) {
    byte[] result = cache.get(key.getBytes());
    handler.handle(result);
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
    executor.shutdown();
  }
  public static class Serializer implements CacheSerializer<byte[]> {

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