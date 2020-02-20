/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.MarkerManager;

/**
 * The background thread that keeps track of the configurations and keeps the executor services in sync.
 */
public class BurstAndUpdateThread extends Thread {

  public static final String name = BurstAndUpdateThread.class.getSimpleName();
  private static final Logger logger = LogManager.getLogger();
  /**
   * The warm up interval
   */
  private static final long CONNECTOR_UPDATE_INTERVAL = TimeUnit.MINUTES.toMillis(2);
  private static BurstAndUpdateThread instance;
  private volatile Handler<AsyncResult<Void>> initializeHandler;

  private BurstAndUpdateThread(Handler<AsyncResult<Void>> handler) throws NullPointerException {
    super(name);
    if (instance != null) {
      throw new IllegalStateException("Singleton warm-up thread has already been instantiated.");
    }
    initializeHandler = handler;
    BurstAndUpdateThread.instance = this;
    this.setDaemon(true);
    this.start();
    logger.info("Started thread {}", name);
  }

  public static void initialize(Handler<AsyncResult<Void>> handler) {
    if (instance == null) {
      instance = new BurstAndUpdateThread(handler);
    }
  }

  private synchronized void onConnectorList(AsyncResult<List<Connector>> ar) {
    if (ar.failed()) {
      logger.error("Failed to receive connector list", ar.cause());
      return;
    }
    final List<Connector> connectorList = ar.result();
    final HashMap<String, Connector> connectorMap = new HashMap<>();

    for (final Connector connector : connectorList) {
      if (connector == null || connector.id == null) {
        logger.error("Found null entry (or without ID) in connector list, see stack trace");
        continue;
      }
      connectorMap.put(connector.id, connector);
      try { //Try to initialize the connector client
        RpcClient.getInstanceFor(connector, true);
      } catch (Exception e) {
        logger.error("Error while trying to get RpcClient for connector with ID " + connector.id, e);
      }
    }

    //Run the warm-up for the lambda connectors which have a warmUpCount > 0 and do some updates
    for (final RpcClient client : RpcClient.getAllInstances()) {
      final Connector oldConnector = client.getConnector();
      if (oldConnector == null) {
        //The client is already destroyed.
        continue;
      }

      if (!connectorMap.containsKey(oldConnector.id)) {
        //Client needs to be destroyed, the connector configuration with the given ID has been removed.
        try {
          client.destroy();
        } catch (Exception e) {
          logger.error("Unexpected exception while destroying RPC client", e);
        }
        continue;
      }

      Connector newConnector = connectorMap.get(oldConnector.id);
      if (!oldConnector.equalTo(newConnector)) {
        try {
          //Update the connector configuration of the client
          client.setConnectorConfig(newConnector);
        }
        catch (Exception e) {
          logger.error("Unexpected exception while trying to update connector configuration", e);
          continue;
        }
      }
      else {
        //Use the existing connector configuration instance
        newConnector = oldConnector;
      }

      if (newConnector.remoteFunction.warmUp > 0) {
        final int minInstances = newConnector.remoteFunction.warmUp;
        try {
          final AtomicInteger requestCount = new AtomicInteger(minInstances);
          logger.info("Send {} health status requests to connector '{}'", requestCount, newConnector.id);
          synchronized (requestCount) {
            for (int i = 0; i < minInstances; i++) {
              HealthCheckEvent healthCheck = new HealthCheckEvent()
                  .withMinResponseTime(200);
              //Just generate a stream ID here as the stream actually "begins" here
              final String healthCheckStreamId = UUID.randomUUID().toString();
              healthCheck.setStreamId(healthCheckStreamId);
              client.execute(MarkerManager.getMarker(healthCheckStreamId), healthCheck, r -> {
                if (r.failed()) {
                  logger.error("Warmup-healtcheck failed for connector with ID " + oldConnector.id, r.cause());
                }
                synchronized (requestCount) {
                  requestCount.decrementAndGet();
                  requestCount.notifyAll();
                }
              });
            }
          }
        }
        catch (Exception e) {
          logger.error("Unexpected exception while trying to send lambda warm-up requests", e);
        }
      }
    }

    //Call the service initialization handler in the first run
    if (initializeHandler != null) {
      initializeHandler.handle(Future.succeededFuture());
      initializeHandler = null;
    }
  }

  @Override
  public void run() {
    //Stay alive as long as the executor (our parent) is alive.
    //noinspection InfiniteLoopStatement
    while (true) {
      try {
        final long start = Service.currentTimeMillis();
        Service.connectorConfigClient.getAll(null, this::onConnectorList);
        final long end = Service.currentTimeMillis();
        final long runtime = end - start;
        if (runtime < CONNECTOR_UPDATE_INTERVAL) {
          Thread.sleep(CONNECTOR_UPDATE_INTERVAL - runtime);
        }
      } catch (InterruptedException e) {
        //We expect that this may happen and ignore it.
      } catch (Exception e) {
        logger.error("Unexpected error in BurstAndUpdateThread", e);
      }
    }
  }
}
