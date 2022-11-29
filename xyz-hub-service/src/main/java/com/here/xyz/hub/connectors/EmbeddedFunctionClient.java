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

import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.connectors.SimulatedContext;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.MaintenanceClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.LimitedOffHeapQueue.PayloadVanishedException;
import com.here.xyz.psql.DatabaseMaintainer;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import javax.naming.NoPermissionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class EmbeddedFunctionClient extends RemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();
  /**
   * The thread pool being used for running calls to embedded connectors asynchronously.
   */
  private ExecutorService embeddedExecutor;

  private static final AtomicReference<MaintenanceClient> maintenanceClientRef = new AtomicReference<>();

  EmbeddedFunctionClient(Connector connectorConfig) {
    super(connectorConfig);
    MaintenanceClient maintenanceClient = maintenanceClientRef.get();
    if (maintenanceClient== null) {
      maintenanceClient = new MaintenanceClient();
      if (maintenanceClientRef.compareAndSet(null, maintenanceClient)) {
        final Map<String, Object> params = connectorConfig.params;
        final Object raw_ecps = params.get("ecps");
        if (raw_ecps instanceof String) {
          final String ecps = (String) raw_ecps;
          final RemoteFunctionConfig raw_remoteFunction = connectorConfig.remoteFunctions.get(Service.configuration.ENVIRONMENT_NAME);
          if (raw_remoteFunction instanceof RemoteFunctionConfig.Embedded) {
            final RemoteFunctionConfig.Embedded remoteFunction =(RemoteFunctionConfig.Embedded) raw_remoteFunction;
            try {
              maintenanceClient.initializeOrUpdateDatabase(connectorConfig.id, ecps, remoteFunction.env.get("ECPS_PHRASE"));
            } catch (Exception e) {
              logger.error(e);
            }
          }
        }
      }
    }
  }

  @Override
  synchronized protected void setConnectorConfig(final Connector newConnectorConfig) throws NullPointerException, IllegalArgumentException {
    super.setConnectorConfig(newConnectorConfig);
    shutdown(embeddedExecutor);
    createExecutorService(newConnectorConfig.id);
  }

  private void createExecutorService(String connectorId) {
    if (!(getConnectorConfig().getRemoteFunction() instanceof RemoteFunctionConfig.Embedded)) {
      throw new IllegalArgumentException("Invalid remoteFunctionConfig argument, must be an instance of Embedded");
    }
    int maxConnections = getMaxConnections();
    embeddedExecutor = new ThreadPoolExecutor(8, maxConnections, 10, TimeUnit.MINUTES,
        new SynchronousQueue<>(), Core.newThreadFactory("embeddedRfc-" + connectorId));
  }

  @Override
  void destroy() {
    super.destroy();
    shutdown(embeddedExecutor);
  }

  private static void shutdown(ExecutorService execService) {
    if (execService == null) return;
    //Shutdown the executor service after the request timeout
    //TODO: Use CompletableFuture.delayedExecutor() after switching to Java 9
    new Thread(() -> {
      try {
        Thread.sleep(MAX_REQUEST_TIMEOUT);
      }
      catch (InterruptedException ignored) {}
      execService.shutdownNow();
    }).start();
  }

  @Override
  protected void invoke(FunctionCall fc, Handler<AsyncResult<byte[]>> callback) {
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().getRemoteFunction();
    Marker marker = fc.marker;
    byte[] payload;
    try {
      payload = fc.getPayload();
    }
    catch (PayloadVanishedException e) {
      callback.handle(Future.failedFuture(new HttpException(TOO_MANY_REQUESTS, "Remote function is busy or cannot be invoked.")));
      return;
    }
    logger.info(marker, "Invoke embedded lambda '{}' for event: {}", remoteFunction.id, new String(payload));

    embeddedExecutor.execute(() -> {
      String className = null;
      try {
        className = ((Connector.RemoteFunctionConfig.Embedded) remoteFunction).className;
        final Class<?> mainClass = Class.forName(className);
        final RequestStreamHandler reqHandler = (RequestStreamHandler) mainClass.newInstance();
        if (reqHandler instanceof AbstractConnectorHandler) {
          ((AbstractConnectorHandler) reqHandler).setEmbedded(true);
        }
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        reqHandler.handleRequest(new ByteArrayInputStream(payload), output,
            new EmbeddedContext(marker, remoteFunction.id,
                ((Connector.RemoteFunctionConfig.Embedded) remoteFunction).env));
        logger.info(marker, "Handling response of embedded lambda call to '{}'.", remoteFunction.id);
        byte[] responseBytes = output.toByteArray();
        callback.handle(Future.succeededFuture(responseBytes));
      }
      catch (ClassNotFoundException e) {
        logger.error(marker, "Configuration error, the specified class '{}' was not found {}", className, e);
        callback.handle(Future.failedFuture(e));
      }
      catch (NoClassDefFoundError e) {
        logger.error(marker, "Configuration error, the specified class '{}' is referring to '{}' which does not exist", className,
            e.getMessage());
        callback.handle(Future.failedFuture(e));
      }
      catch (Throwable e) {
        logger.error(marker, "Exception occurred, while trying to execute embedded lambda with id '{}' {}", remoteFunction.id, e);
        callback.handle(Future.failedFuture(e));
      }
    });
  }

  /**
   * Context used by embedded lambda connectors.
   */
  public static class EmbeddedContext extends SimulatedContext {

    private static final Logger logger = LogManager.getLogger();
    private final Marker marker;

    public EmbeddedContext(Marker marker, String functionName, Map<String, String> environmentVariables) {
      super(functionName, environmentVariables);
      this.marker = marker;
    }

    @Override
    public void log(String string) {
      logger.info(marker, string);
    }

    @Override
    public void log(byte[] bytes) {
      logger.info(marker, "bytes: {}", bytes);
    }
  }
}
