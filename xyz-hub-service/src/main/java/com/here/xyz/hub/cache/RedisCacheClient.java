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

import com.here.xyz.hub.Service;
import io.vertx.core.Future;
import io.vertx.core.net.NetClientOptions;
import io.vertx.redis.client.Command;
import io.vertx.redis.client.Redis;
import io.vertx.redis.client.RedisOptions;
import io.vertx.redis.client.Request;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RedisCacheClient implements CacheClient {

  private static CacheClient instance;
  private static final Logger logger = LogManager.getLogger();
  private ThreadLocal<Redis> redis;
  private String connectionString = Service.configuration.getRedisUri();
  RedisOptions config = new RedisOptions()
      .setConnectionString(connectionString)
      .setNetClientOptions(new NetClientOptions()
          .setTcpKeepAlive(true)
          .setIdleTimeout(30)
          .setConnectTimeout(2000));
  private static final String RND = UUID.randomUUID().toString();

  private RedisCacheClient() {
    //Use redis auth token when available
    if (Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN != null)
      config.setPassword(Service.configuration.XYZ_HUB_REDIS_AUTH_TOKEN);
    redis = ThreadLocal.withInitial(() -> Redis.createClient(Service.vertx, config));
  }

  public static synchronized CacheClient getInstance() {
    if (instance != null)
      return instance;

    if (Service.configuration.getRedisUri() == null)
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
  public Future<byte[]> get(String key) {
    Request req = Request.cmd(Command.GET).arg(key);
    return getClient().send(req)
        .compose(response -> Future.succeededFuture(response == null ? null : response.toBytes()), t -> {
          logger.warn("Error when trying to read key " + key + " from redis cache", t);
          return Future.succeededFuture(null);
        });
  }

  @Override
  public void set(String key, byte[] value, long ttl) {
    Request req = Request.cmd(Command.SET).arg(key).arg(value).arg("EX").arg(ttl);
    getClient().send(req).onComplete(ar -> {
      //SET command was executed. Nothing to do here.
      if (ar.failed()) {
        logger.warn("Error when trying to put key " + key + " to redis cache", ar.cause());
      }
    });
  }

  @Override
  public void remove(String key) {
    Request req = Request.cmd(Command.DEL).arg(key);
    getClient().send(req).onComplete(ar -> {
      //DEL command was executed. Nothing to do here.
      if (ar.failed()) {
        logger.warn("Error removing cache entry for key {}.", key, ar.cause());
      }
    });
  }

  @Override
  public void shutdown() {
    if (redis != null)
      getClient().close();
  }

  /**
   * Acquires the lock on the specified key and sets a ttl in seconds.
   * Implementation based on https://redis.io/topics/distlock for single Redis instances
   * @param key the key which the lock will be acquired
   * @param ttl the expiration time in seconds for this lock be automatically released
   * @return true in case the lock was successfully acquired. False otherwise.
   */
  public boolean acquireLock(String key, long ttl) {
    Request req = Request.cmd(Command.SET).arg(key).arg(RND).arg("NX").arg("EX").arg(ttl);
    CompletableFuture<Boolean> f = new CompletableFuture<>();
    getClient().send(req).onComplete(ar -> {
      if (ar.failed()) {
        logger.warn("Error acquiring lock for key {}.", key, ar.cause());
        f.complete(false);
      }
      else if (ar.result() == null)
        f.complete(false);
      else
        f.complete("OK".equals(ar.result().toString()));
    });
    try {
      return f.get();
    }
    catch (ExecutionException | InterruptedException e) {
      return false;
    }
  }

  /**
   * Releases the lock acquired by acquireLock. The key must match with the lock acquired previously.
   * @param key the key which the lock was acquired
   */
  public void releaseLock(String key) {
    final String luaScript = "if redis.call(\"get\",KEYS[1]) == ARGV[1] then return redis.call(\"del\",KEYS[1]) else return 0 end";
    Request req = Request.cmd(Command.EVAL).arg(luaScript).arg(1).arg(key).arg(RND);
    getClient().send(req).onComplete(ar -> {
      if (ar.failed()) {
        logger.warn("Error releasing lock for key {}.", key, ar.cause());
      }
    });
  }
}
