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
import io.vertx.core.buffer.Buffer;
import io.vertx.redis.RedisClient;
import io.vertx.redis.RedisOptions;
import io.vertx.redis.op.SetOptions;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisCacheClient implements CacheClient {

  private static final Logger logger = LogManager.getLogger();
  private ThreadLocal<RedisClient> redis;

  public RedisCacheClient() {
    redis = ThreadLocal.withInitial(() -> {
      RedisOptions config = new RedisOptions()
          .setHost(Service.configuration.XYZ_HUB_REDIS_HOST)
          .setPort(Service.configuration.XYZ_HUB_REDIS_PORT);

      // use redis auth token when available
      if (!StringUtils.isEmpty(Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN)) {
        config.setAuth(Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN);
      }

      config.setTcpKeepAlive(true);
      config.setConnectTimeout(2000);
      return RedisClient.create(Service.vertx, config);
    });
  }

  public static CacheClient get() {
    if (Service.configuration.XYZ_HUB_REDIS_HOST == null) {
      return new NoopCacheClient();
    }
    try {
      return new RedisCacheClient();
    } catch (Exception e) {
      logger.error("Error when trying to create the Redis client.", e);
      return new NoopCacheClient();
    }
  }

  protected RedisClient getClient() {
    return redis.get();
  }

  @Override
  public void get(String key, Handler<byte[]> handler) {
    getClient().getBinary(key, asyncResult -> {
      if (asyncResult.failed()) {
//				logger.error("Error when trying to read key " + key + " from redis cache", asyncResult.cause());
      }
      final Buffer result = asyncResult.result();
      handler.handle(result == null ? null : result.getBytes());
    });
  }

  @Override
  public void set(String key, byte[] value, long ttl) {
    getClient().setBinaryWithOptions(key, Buffer.buffer(value), new SetOptions().setEX(ttl), asyncResult -> {
      //set command was executed. Nothing to do here.
      if (asyncResult.failed()) {
        //logger.error("Error when trying to put key " + key + " to redis cache", asyncResult.cause());
      }
    });
  }

  @Override
  public void remove(String key) {
    getClient().del(key, response -> {
      //del command was executed. Nothing to do here.
      logger.warn("Error removing cache entry for key {}.", key);
    });
  }

  @Override
  public void shutdown() {
    if (redis != null) {
      getClient().close(r -> {
        synchronized (this) {
          this.notify();
        }
      });
    }
    synchronized (this) {
      try {
        this.wait(2000);
      } catch (InterruptedException e) {
        //Nothing to do.
      }
    }
  }

}
