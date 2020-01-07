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
  private static final long WARM_UP_INTERVAL_MILLISECONDS = TimeUnit.MINUTES.toMillis(2);
  private static final long CONNECTOR_UPDATE_INTERVAL = TimeUnit.SECONDS.toMillis(10);
  private static BurstAndUpdateThread instance;

  private BurstAndUpdateThread() throws NullPointerException {
    super(name);
    if (instance != null) {
      throw new IllegalStateException("Singleton warm-up thread has already been instantiated.");
    }
    BurstAndUpdateThread.instance = this;
    this.setDaemon(true);
    this.start();
    logger.info("Starting thread {}", name);
  }

  public static void initialize() {
    if (instance == null) {
      instance = new BurstAndUpdateThread();
    }
  }

  private synchronized void onConnectorList(AsyncResult<List<Connector>> ar) {
    if (ar.failed()) {
      //TODO: Handle errors, but for now we may as well ignore errors.
      logger.error("Failed to receive connector list", ar.cause());
      return;
    }
    final List<Connector> connectorList = ar.result();
    final HashMap<String, Connector> connectorMap = new HashMap<>();

    for (final Connector connector : connectorList) {
      connectorMap.put(connector.id, connector);
      try { //Try to initialize the connector client
        RpcClient.getInstanceFor(connector);
      } catch (Exception ignored) {
      }
    }

    //Run the warm-up for the lambda connectors which have a warmUpCount > 0 and do some updates
    for (final RpcClient client : RpcClient.getAllInstances()) {
      final String connectorId = client.connector.id;
      if (!connectorMap.containsKey(connectorId)) {
        //This will shutdown all lambda clients for that connector!
        RpcClient.destroyInstance(client);
        continue;
      }
      Connector connector = connectorMap.get(connectorId);
      if (!client.connector.equalTo(connector)) {
        //Update the connector configuration of the client
        client.updateConnectorConfig(connector);
      } else {
        //Take the existing connector configuration instance
        connector = client.connector;
      }

      if (connector.remoteFunction.warmUp > 0) {
        final int minInstances = connector.remoteFunction.warmUp;

        try {
          final AtomicInteger requestCount = new AtomicInteger(minInstances);
          logger.info("Send {} health status requests to connector '{}'", requestCount, connector.id);
          synchronized (requestCount) {
            for (int i = 0; i < minInstances; i++) {
              HealthCheckEvent healthCheck = new HealthCheckEvent()
                  .withMinResponseTime(200);
              // Just generate a stream ID here as the stream actually "begins" here
              final String pseudoStreamId = UUID.randomUUID().toString();
              healthCheck.setStreamId(pseudoStreamId);
              client.execute(MarkerManager.getMarker(pseudoStreamId), healthCheck, r -> {
                synchronized (requestCount) {
                  requestCount.decrementAndGet();
                  requestCount.notifyAll();
                }
              });
            }
          }
        } catch (Exception e) {
          logger.error("Unexpected exception while trying to send lambda warm-up requests", e);
        }
      }
    }
  }

  @Override
  public void run() {
    try {
      Thread.sleep(CONNECTOR_UPDATE_INTERVAL);
    } catch (InterruptedException ignored) {
    }
    // Stay alive as long as the executor (our parent) is alive.
    while (true) {
      try {
        final long start = System.currentTimeMillis();

        Service.connectorConfigClient.getAll(null, this::onConnectorList);

        final long end = System.currentTimeMillis();
        final long runtime = end - start;
        if (runtime < WARM_UP_INTERVAL_MILLISECONDS) {
          Thread.sleep(WARM_UP_INTERVAL_MILLISECONDS - runtime);
        }
      } catch (InterruptedException e) {
        // We expect that this may happen and ignore it.
      } catch (Exception e) {
        logger.error("Unexpected error in lambda executor background thread", e);
      }
    }
  }
}
