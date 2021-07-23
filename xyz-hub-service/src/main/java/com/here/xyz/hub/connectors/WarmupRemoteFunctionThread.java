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

package com.here.xyz.hub.connectors;

import com.here.xyz.events.HealthCheckEvent;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.cache.RedisCacheClient;
import com.here.xyz.hub.connectors.models.Connector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.MarkerManager.Log4jMarker;

/**
 *
 */
public class WarmupRemoteFunctionThread extends Thread {

  private static final String name = WarmupRemoteFunctionThread.class.getSimpleName();
  private static final Logger logger = LogManager.getLogger();
  private static final String RFC_WARMUP_CACHE_KEY = "RFC_WARMUP_CACHE_KEY";

  // minimum amount of seconds to wait between each run, also used as mutex expiration period
  private static final long MIN_WAIT_TIME_SECONDS = 120;

  // interval is a random number of seconds varying between 120 and 180.
  private static final long CONNECTOR_WARMUP_INTERVAL = TimeUnit.SECONDS.toMillis(MIN_WAIT_TIME_SECONDS) + TimeUnit.SECONDS.toMillis((long) (Math.random() * 60));

  private static WarmupRemoteFunctionThread instance;
  private volatile Handler<AsyncResult<Void>> initializeHandler;


  private WarmupRemoteFunctionThread(Handler<AsyncResult<Void>> handler) throws NullPointerException {
    super(name);
    if (instance != null) {
      throw new IllegalStateException("Singleton warmup thread has already been instantiated.");
    }
    initializeHandler = handler;
    WarmupRemoteFunctionThread.instance = this;
    this.setDaemon(true);
    this.start();
    logger.info("Started warmup thread {}", name);
  }

  public static void initialize(Handler<AsyncResult<Void>> handler) {
    if (instance == null) {
      instance = new WarmupRemoteFunctionThread(handler);
    }
  }

  private synchronized void executeWarmup() {
    final Map<String, Connector> remoteFunctionsMap = new HashMap<>();

    // prepare the list of remote functions to be called, select max warmup value for each remote function
    for (final RpcClient client : RpcClient.getAllInstances()) {
      if (client.getConnector().getRemoteFunction().warmUp > 0) {
        String remoteFunctionId = client.getConnector().getRemoteFunction().id;
        Connector connector = compareWarmup(client.getConnector(), remoteFunctionsMap.get(remoteFunctionId));

        if (connector != null) {
          remoteFunctionsMap.put(remoteFunctionId, connector);
        }
      }
    }

    remoteFunctionsMap.forEach((remoteFunctionId, connector) -> {
      final int minInstances = connector.getRemoteFunction().warmUp;
      try {
        final AtomicInteger requestCount = new AtomicInteger(minInstances);
        logger.info("Send {} health status requests to remote function '{}'", requestCount, connector.getRemoteFunction().id);
        synchronized (requestCount) {
          for (int i = 0; i < minInstances; i++) {
            HealthCheckEvent healthCheck = new HealthCheckEvent().withMinResponseTime(200);
            //Just generate a stream ID here as the stream actually "begins" here
            final String healthCheckStreamId = UUID.randomUUID().toString();
            healthCheck.setStreamId(healthCheckStreamId);
            RpcClient.getInstanceFor(connector).execute(new Log4jMarker(healthCheckStreamId), healthCheck, r -> {
              if (r.failed()) {
                logger.warn("Warmup-healtcheck failed for remote function with ID " + remoteFunctionId, r.cause());
              }
              synchronized (requestCount) {
                requestCount.decrementAndGet();
                requestCount.notifyAll();
              }
            });
          }
        }
      }
      catch (IllegalStateException e) {
        logger.info("Exception when retrieving RpcClient for connector.", e);
      }
      catch (Exception e) {
        logger.error("Unexpected exception while trying to send warm-up requests", e);
      }
    });
  }

  private Connector compareWarmup(Connector connector1, Connector connector2) {
    if (connector1 == null && connector2 == null) return null;
    if (connector1 == null) return connector2;
    if (connector2 == null) return connector1;
    return connector1.getRemoteFunction().warmUp > connector2.getRemoteFunction().warmUp ? connector1 : connector2;
  }

  private boolean isWarmupExpired() {
    CompletableFuture<Boolean> f = new CompletableFuture<>();
    RedisCacheClient.getInstance().get(RFC_WARMUP_CACHE_KEY).onSuccess(r -> {
      f.complete(r == null);
    });

    try {
      return f.get();
    }
    catch (ExecutionException | InterruptedException e) {

      return false;
    }
  }

  private void setWarmupFlag() {
    RedisCacheClient.getInstance().set(RFC_WARMUP_CACHE_KEY, new byte[0], MIN_WAIT_TIME_SECONDS);
  }

  @Override
  public void run() {
    //Stay alive as long as the executor (our parent) is alive.
    //noinspection InfiniteLoopStatement
    while (true) {
      try {
        final long start = Core.currentTimeMillis();
        if (isWarmupExpired()) {
          setWarmupFlag();
          executeWarmup();
        }
        final long end = Core.currentTimeMillis();
        final long runtime = end - start;
        if (runtime < CONNECTOR_WARMUP_INTERVAL) {
          Thread.sleep(CONNECTOR_WARMUP_INTERVAL - runtime);
        }
      } catch (InterruptedException e) {
        //We expect that this may happen and ignore it.
      } catch (Exception e) {
        logger.error("Unexpected error in WarmupRemoteFunctionThread", e);
      }

      //Call the service initialization handler in the first run
      if (initializeHandler != null) {
        initializeHandler.handle(Future.succeededFuture());
        initializeHandler = null;
      }
    }
  }
}
