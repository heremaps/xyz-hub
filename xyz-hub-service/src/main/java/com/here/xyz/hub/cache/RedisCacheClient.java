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
import io.vertx.core.net.NetClientOptions;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisCacheClient implements CacheClient {

  private static CacheClient instance;
  private static final Logger logger = LogManager.getLogger();
  private ThreadLocal<Redis> redis;
  private String connectionString = Service.configuration.XYZ_HUB_REDIS_URI;

  private RedisCacheClient() {
    redis = ThreadLocal.withInitial(() -> {
      RedisOptions config = new RedisOptions()
          .setConnectionString(connectionString)
          .setNetClientOptions(new NetClientOptions()
              .setTcpKeepAlive(true)
              .setIdleTimeout(30)
              .setConnectTimeout(2000));

      //Use redis auth token when available
      if (!StringUtils.isEmpty(Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN))
        config.setPassword(Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN);

      return Redis.createClient(Service.vertx, config);
    });
  }

  public static synchronized CacheClient getInstance() {
    if (instance != null)
      return instance;

    if (Service.configuration.XYZ_HUB_REDIS_URI == null)
      instance = new NoopCacheClient();
    else {
      try {
        instance = new RedisCacheClient();
      }
      catch (Exception e) {
        logger.error("Error when trying to create the Redis client.", e);
        instance = new NoopCacheClient();
      }
    }
    return instance;
  }

  protected Redis getClient() {
    return redis.get();
  }

  @Override
  public void get(String key, Handler<byte[]> handler) {
    Request req = Request.cmd(Command.GET).arg(key);
    getClient().send(req).onComplete(ar -> {
      if (ar.failed()) {
        //logger.warn("Error when trying to read key " + key + " from redis cache", ar.cause());
      }
      handler.handle(ar.result() == null ? null : ar.result().toBytes());
    });
  }

  @Override
  public void set(String key, byte[] value, long ttl) {
    Request req = Request.cmd(Command.SET).arg(key).arg(value).arg("EX").arg(ttl);
    getClient().send(req).onComplete(ar -> {
      //SET command was executed. Nothing to do here.
      if (ar.failed()) {
        //logger.warn("Error when trying to put key " + key + " to redis cache", ar.cause());
      }
    });
  }

  @Override
  public void remove(String key) {
    Request req = Request.cmd(Command.DEL).arg(key);
    getClient().send(req).onComplete(ar -> {
      //DEL command was executed. Nothing to do here.
      if (ar.failed()) {
        //logger.warn("Error removing cache entry for key {}.", key, ar.cause());
      }
    });
  }

  @Override
  public void shutdown() {
    if (redis != null)
      getClient().close();
  }

}
