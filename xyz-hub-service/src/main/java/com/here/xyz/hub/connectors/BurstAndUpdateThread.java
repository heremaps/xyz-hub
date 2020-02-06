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
  @SuppressWarnings("unused")
  private static final BurstAndUpdateThread instance = new BurstAndUpdateThread();

  private BurstAndUpdateThread() throws NullPointerException {
    super(name);
    this.setDaemon(true);
    this.start();
    logger.info("Starting thread {}", name);
  }

  public static void initialize() {
    // It is better to use the static constructor for the singleton, the compile knows now that it is a singleton.
  }

  private synchronized void onConnectorList(final AsyncResult<List<Connector>> ar) {
    if (ar.failed()) {
      //TODO: Handle errors, but for now we may as well ignore errors.
      logger.error("Failed to receive connector list", ar.cause());
      return;
    }
    final List<Connector> newConnectorList = ar.result();

    final HashMap<String, Connector> newConnectorConfigMap = new HashMap<>();
    for (final Connector connectorConfig : newConnectorList) {
      if (connectorConfig == null || connectorConfig.id == null) {
        logger.error("Found null entry (or without ID) in connector list, see stack trace", new IllegalStateException());
        continue;
      }
      newConnectorConfigMap.put(connectorConfig.id, connectorConfig);
      try { //Try to initialize the connector client
        RpcClient.getInstanceFor(connectorConfig);
      } catch (Exception ignored) {
      }
    }

    // Run the warm-up for the lambda connectors which have a warmUpCount > 0 and do some updates
    for (final RpcClient client : RpcClient.getAllInstances()) {
      Connector connectorConfig = client.getConnectorConfig();
      if (connectorConfig == null) {
        // The client is already destroyed.
        continue;
      }

      // Client needs to be destroyed, the connector configuration with the given ID has been removed.
      if (!newConnectorConfigMap.containsKey(connectorConfig.id)) {
        try {
          client.destroy();
        } catch (Exception e) {
          logger.error("Unexpected exception while destroying RPC client", e);
        }
        continue;
      }

      {
        final Connector newConnectorConfig = newConnectorConfigMap.get(connectorConfig.id);
        if (!connectorConfig.equalTo(newConnectorConfig)) {
          try {
            client.setConnectorConfig(newConnectorConfig);
          } catch (Exception e) {
            logger.error("Unexpected exception while trying to update connector configuration", e);
            // TODO: Should we destroy the client? I should be re-created anyway later when RpcClient.getInstanceFor is called?
            continue;
          }
          connectorConfig = newConnectorConfig;
        }
      }

      if (connectorConfig.remoteFunction.warmUp > 0) {
        final int minInstances = connectorConfig.remoteFunction.warmUp;
        try {
          final AtomicInteger requestCount = new AtomicInteger(minInstances);
          logger.info("Send {} health status requests to connector '{}'", requestCount, connectorConfig.id);
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
        final long start = Service.currentTimeMillis();
        Service.connectorConfigClient.getAll(null, this::onConnectorList);
        final long end = Service.currentTimeMillis();
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
