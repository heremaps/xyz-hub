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

package com.here.xyz.hub.connectors;

import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.settings.EnvironmentVariableOverrides;
import com.here.xyz.hub.config.settings.EnvironmentVariableOverrides.VariableOverrideException;
import com.here.xyz.hub.config.settings.SpaceStorageMatchingMap;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.rest.health.HealthApi;
import com.here.xyz.hub.util.health.checks.RemoteFunctionHealthCheck;
import io.vertx.core.AsyncResult;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager.Log4jMarker;

/**
 * The background thread that keeps track of the configurations and keeps the executor services in sync.
 */
public class ConfigUpdateThread extends Thread {

  private static final String name = ConfigUpdateThread.class.getSimpleName();
  private static final Logger logger = LogManager.getLogger();
  private static final long CONFIG_UPDATE_INTERVAL = TimeUnit.MINUTES.toMillis(2);
  private static final long CONNECTOR_UNHEALTHY_THRESHOLD = 3;

  private static final Marker MARKER = new Log4jMarker(name);

  private static ConfigUpdateThread instance;
  private CompletableFuture<Void> initializeFuture;

  private ConfigUpdateThread(CompletableFuture<Void> initializeFuture) throws NullPointerException {
    super(name);
    if (instance != null) {
      throw new IllegalStateException("Singleton ConfigUpdateThread has already been instantiated.");
    }
    this.initializeFuture = initializeFuture;
    ConfigUpdateThread.instance = this;
    this.setDaemon(true);
    this.start();
    logger.info("Started thread {}", name);
  }

  public static CompletableFuture<Void> initialize() {
    if (instance == null) {
      CompletableFuture<Void> future = new CompletableFuture<>();
      instance = new ConfigUpdateThread(future);
      return future;
    }

    return CompletableFuture.completedFuture(null);
  }

  private void performConnectorUpdates() {
    try {
      Service.connectorConfigClient.getAll(MARKER, this::onConnectorList);
    }
    catch (Exception e) {
      logger.error(MARKER, "Unexpected error during connector update", e);
    }
  }

  private void performSettingsUpdates() {
    Service.settingsConfigClient.get(MARKER, EnvironmentVariableOverrides.class.getSimpleName()).onSuccess(vars -> {
      if (vars == null)
        logger.info("No environment variable overrides existing. Skipping settings update.");
      else try {
        ((EnvironmentVariableOverrides) vars).applyOverrides();
      }
      catch (VariableOverrideException e) {
        logger.error(MARKER, "Error during override of environment settings.", e);
      }
      catch (Exception e) {
        logger.error(MARKER, "Unexpected error during EnvironmentVariableOverrides update.", e);
      }
    });

    Service.settingsConfigClient.get(MARKER, SpaceStorageMatchingMap.class.getSimpleName()).onSuccess(settings -> {
      if (settings == null)
        logger.info("No space connector matching map existing. Skipping settings update.");
      else try {
        ((SpaceStorageMatchingMap) settings).updatePatterns();
      }
      catch (Exception e) {
        logger.error(MARKER, "Unexpected error during SpaceIdConnectorIdMatching update.", e);
      }
    });
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

      if (connector.active) {
        connectorMap.put(connector.id, connector);
        try { //Try to initialize the connector client
          RpcClient.getInstanceFor(connector, true);
        }
        catch (Exception e) {
          logger.error("Error while trying to get / create RpcClient for connector with ID " + connector.id, e);
        }
      }
    }

    // Do some updates
    for (final RpcClient client : RpcClient.getAllInstances()) {
      final Connector oldConnector = client.getConnector();
      if (oldConnector == null) {
        //The client is already destroyed.
        continue;
      }

      Connector newConnector = connectorMap.get(oldConnector.id);
      if (Service.configuration.ENABLE_CONNECTOR_HEALTH_CHECKS && newConnector != null && !newConnector.skipAutoDisable
          && !Service.configuration.defaultStorageIds.contains(oldConnector.id)) {
        RemoteFunctionHealthCheck rfcHc = HealthApi.rfcHcAggregator.getRfcHealthCheck(oldConnector.id);
        if (rfcHc != null) {
          //When the connector is responding with unhealthy status, disable it momentarily, until next ConfigUpdateThread round.
          int consecutiveFailures = rfcHc.getConsecutiveFailures();
          if (consecutiveFailures >= CONNECTOR_UNHEALTHY_THRESHOLD) {
            logger.warn("For connector {} there are {} unhealthy health-checks. Max threshold is {}.", oldConnector.id,
                consecutiveFailures, CONNECTOR_UNHEALTHY_THRESHOLD);
            connectorMap.remove(oldConnector.id);
          }
        }
      }

      if (!connectorMap.containsKey(oldConnector.id)) {
        //Client needs to be destroyed, the connector configuration with the given ID has been removed.
        try {
          logger.warn("Connector {} was removed or deactivated. Destroying the according client.", oldConnector.id);
          client.destroy();
        }
        catch (Exception e) {
          logger.error("Unexpected exception while destroying RPC client", e);
        }
        continue;
      }

      if (!oldConnector.equalTo(newConnector)) {
        try {
          //Update the connector configuration of the client
          logger.info("The incoming connector config for \"{}\" is different from the existing one. Updating the according RPC client.", newConnector.id);
          client.setConnectorConfig(newConnector);
        }
        catch (Exception e) {
          logger.error("Unexpected exception while trying to update connector configuration for " + newConnector.id, e);
        }
      }
    }

    //Call the service initialization handler in the first run
    if (initializeFuture != null) {
      initializeFuture.complete(null);
      initializeFuture = null;
    }
  }

  private void performUpdates() {
    performConnectorUpdates();
    performSettingsUpdates();
  }

  @Override
  public void run() {
    //Stay alive as long as the executor (our parent) is alive.
    //noinspection InfiniteLoopStatement
    while (true) {
      try {
        final long start = Core.currentTimeMillis();
        performUpdates();
        final long end = Core.currentTimeMillis();
        final long runtime = end - start;
        if (runtime < CONFIG_UPDATE_INTERVAL) {
          Thread.sleep(CONFIG_UPDATE_INTERVAL - runtime);
        }
      } catch (InterruptedException e) {
        //We expect that this may happen and ignore it.
      } catch (Exception e) {
        logger.error(MARKER, "Unexpected error in ConfigUpdateThread", e);
      }
    }
  }
}
