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

import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.connectors.SimulatedContext;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public final class EmbeddedFunctionClient extends RemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();
  /**
   * The thread pool being used for running calls to embedded connectors asynchronously.
   */
  private ExecutorService embeddedExecutor;

  EmbeddedFunctionClient(Connector connectorConfig) {
    super(connectorConfig);
  }

  @Override
  synchronized void setConnectorConfig(final Connector newConnectorConfig) throws NullPointerException, IllegalArgumentException {
    super.setConnectorConfig(newConnectorConfig);
    if (embeddedExecutor != null) {
      shutdown(embeddedExecutor);
    }
    createExecutorService();
  }

  @Override
  synchronized void initialize() {
    super.initialize();
    createExecutorService();
  }

  private void createExecutorService() {
    final Connector connectorConfig = getConnectorConfig();
    if (!(connectorConfig.remoteFunction instanceof RemoteFunctionConfig.Embedded)) {
      throw new IllegalArgumentException("Invalid remoteFunctionConfig argument, must be an instance of Embedded");
    }
    int maxConnections = connectorConfig.getMaxConnectionsPerInstance();
    embeddedExecutor = new ThreadPoolExecutor(8, maxConnections, 10, TimeUnit.MINUTES, new SynchronousQueue<>());
  }

  @Override
  synchronized void destroy() {
    super.destroy();
    if (embeddedExecutor != null) {
      shutdown(embeddedExecutor);
      embeddedExecutor = null;
    }
  }

  private static void shutdown(ExecutorService execService) {
    //Shutdown the executor service after the request timeout
    //TODO: Use CompletableFuture.delayedExecutor() after switching to Java 9
    new Thread(() -> {
      try {
        Thread.sleep(REQUEST_TIMEOUT);
      } catch (InterruptedException ignored) {
      }
      execService.shutdownNow();
    }).start();
  }

  protected void invoke(final Marker marker, final byte[] bytes, final Handler<AsyncResult<byte[]>> callback) {
    final Connector connectorConfig = getConnectorConfig();
    final RemoteFunctionConfig remoteFunction = connectorConfig.remoteFunction;
    logger.info(marker, "Invoke embedded lambda '{}' for event: {}", connectorConfig.remoteFunction.id,
        new String(bytes, StandardCharsets.UTF_8));
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
        reqHandler.handleRequest(new ByteArrayInputStream(bytes), output,
            new EmbeddedContext(marker, remoteFunction.id,
                ((Connector.RemoteFunctionConfig.Embedded) remoteFunction).env));
        logger.info(marker, "Handling response of embedded lambda call to '{}'.", remoteFunction.id);
        byte[] responseBytes = output.toByteArray();
        checkResponseSize(responseBytes);
        callback.handle(Future.succeededFuture(getDecompressed(responseBytes)));
      } catch (ClassNotFoundException e) {
        logger.error(marker, "Configuration error, the specified class '{}' was not found {}", className, e);
        callback.handle(Future.failedFuture(e));
      } catch (NoClassDefFoundError e) {
        logger.error(marker, "Configuration error, the specified class '{}' is referring to '{}' which does not exist", className,
            e.getMessage());
        callback.handle(Future.failedFuture(e));
      } catch (Throwable e) {
        logger
            .error(marker, "Exception occurred, while trying to execute embedded lambda with id '{}' {}", remoteFunction.id, e);
        callback.handle(Future.failedFuture(e));
      }
    });
  }

  /**
   * Context used by embedded lambda connectors.
   */
  private static class EmbeddedContext extends SimulatedContext {

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
