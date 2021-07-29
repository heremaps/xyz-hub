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
import java.util.UUID;
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

  // minimum amount of milliseconds to wait between each run, also used as lock expiration period
  private static final long CONNECTOR_WARMUP_INTERVAL = 120 * 1000;

  private static WarmupRemoteFunctionThread instance;

  private WarmupRemoteFunctionThread(Handler<AsyncResult<Void>> handler) throws NullPointerException {
    super(name);
    if (instance != null) {
      throw new IllegalStateException("Singleton warmup thread has already been instantiated.");
    }
    WarmupRemoteFunctionThread.instance = this;
    this.setDaemon(true);
    this.start();
    handler.handle(Future.succeededFuture());
    logger.info("Started warmup thread {}", name);
  }

  public static void initialize(Handler<AsyncResult<Void>> handler) {
    if (instance == null) {
      instance = new WarmupRemoteFunctionThread(handler);
    }
  }

  private void executeWarmup() {
    // prepare the list of remote functions to be called, select max warmup value for each remote function
    for (final RpcClient client : RpcClient.getAllInstances()) {
      final Connector connector = client.getConnector();
      final int minInstances = connector.getRemoteFunction().warmUp;

      if (minInstances > 0) {
        final String remoteFunctionId = client.getConnector().getRemoteFunction().id;

        try {
          logger.info("Send {} health status requests to remote function '{}'", minInstances, remoteFunctionId);
          for (int i = 0; i < minInstances; i++) {
            HealthCheckEvent healthCheck = new HealthCheckEvent().withMinResponseTime(200);
            //Just generate a stream ID here as the stream actually "begins" here
            final String healthCheckStreamId = UUID.randomUUID().toString();
            healthCheck.setStreamId(healthCheckStreamId);
            RpcClient.getInstanceFor(connector).execute(new Log4jMarker(healthCheckStreamId), healthCheck, r -> {
              if (r.failed()) {
                logger.warn("Warmup-healtcheck failed for remote function with ID " + remoteFunctionId, r.cause());
              }
            });
          }
        } catch (IllegalStateException e) {
          logger.info("Exception when retrieving RpcClient for connector.", e);
        } catch (Exception e) {
          logger.error("Unexpected exception while trying to send warm-up requests", e);
        }
      }
    }
  }

  @Override
  public void run() {
    //Stay alive as long as the executor (our parent) is alive.
    //noinspection InfiniteLoopStatement
    while (true) {
      logger.info("Warm-up cycle started. Interval of: " + CONNECTOR_WARMUP_INTERVAL + " millis");
      try {
        final long start = Core.currentTimeMillis();

        if (acquireLock()) {
          logger.info("Lock acquired, running.");
          executeWarmup();
          logger.info("Warm-up execution is finished.");
        }
        else {
          logger.info("Unable to acquire lock, waiting next round.");
        }

        final long end = Core.currentTimeMillis();
        final long runtime = end - start;
        if (runtime < CONNECTOR_WARMUP_INTERVAL) {
          long nextWarmup = CONNECTOR_WARMUP_INTERVAL - runtime;
          logger.info("Next warm-up in " + nextWarmup + " millis");
          Thread.sleep(nextWarmup);
        }
      } catch (InterruptedException e) {
        //We expect that this may happen and ignore it.
      } catch (Exception e) {
        logger.error("Unexpected error in WarmupRemoteFunctionThread", e);
      }
    }
  }

  private boolean acquireLock() {
    if (RedisCacheClient.getInstance() instanceof RedisCacheClient) {
      RedisCacheClient redisCacheClient = ((RedisCacheClient) RedisCacheClient.getInstance());
      return redisCacheClient.acquireLock(RFC_WARMUP_CACHE_KEY, (CONNECTOR_WARMUP_INTERVAL / 1000) - 1); // expires 1 second earlier
    }

    return true;
  }
}
