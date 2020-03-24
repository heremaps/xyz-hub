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

import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class HTTPFunctionClient extends RemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();

  private volatile ThreadLocal<WebClient> webClient;
  private volatile String url;
  private static volatile List<DestroyableWebClient> webClientsToDestroy = new LinkedList<>();

  HTTPFunctionClient(Connector connectorConfig) {
    super(connectorConfig);
  }

  @Override
  synchronized void setConnectorConfig(final Connector newConnectorConfig) throws NullPointerException, IllegalArgumentException {
    super.setConnectorConfig(newConnectorConfig);
    shutdownWebClient();
    createClient();
  }

  private void createClient() {
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().remoteFunction;
    if (!(remoteFunction instanceof Http)) {
      throw new IllegalArgumentException("Invalid remoteFunctionConfig argument, must be an instance of HTTP");
    }
    Http httpRemoteFunction = (Http) remoteFunction;
    url = httpRemoteFunction.url.toString();
    webClient = ThreadLocal.withInitial(() -> WebClient.create(Service.vertx, new WebClientOptions()
        .setUserAgent(Service.XYZ_HUB_USER_AGENT)
        .setMaxPoolSize(getMaxConnections())));
  }

  private WebClient getWebClient() {
    return webClient.get();
  }

  @Override
  void destroy() {
    super.destroy();
    shutdownWebClient();
  }

  private void shutdownWebClient() {
    if (webClient == null) return;
    //Shutdown the web client after the request timeout
    //TODO: Use CompletableFuture.delayedExecutor() after switching to Java 9
    new Thread(() -> {
      try {
        Thread.sleep(REQUEST_TIMEOUT);
      }
      catch (InterruptedException ignored) {}
      webClientsToDestroy.forEach(wc -> wc.destroy());
      webClientsToDestroy = new LinkedList<>();
    }).start();
  }

  @Override
  protected void invoke(Marker marker, byte[] bytes, boolean fireAndForget, Handler<AsyncResult<byte[]>> callback) {
    //TODO: respect fireAndForget parameter
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().remoteFunction;
    logger.info(marker, "Invoke http remote function '{}' URL is: {} Event size is: {}", remoteFunction.id, url, bytes.length);

    getWebClient().postAbs(url)
        .timeout(REQUEST_TIMEOUT)
        .putHeader("content-type", "application/json; charset=" + Charset.defaultCharset().name())
        .sendBuffer(Buffer.buffer(bytes), ar -> {
          if (ar.failed()) {
            if (ar.cause() instanceof TimeoutException) {
              callback.handle(Future.failedFuture(new HttpException(GATEWAY_TIMEOUT, "Connector timeout error.")));
            } else {
              callback.handle(Future.failedFuture(ar.cause()));
            }
          } else {
            byte[] responseBytes = ar.result().body().getBytes();
            callback.handle(Future.succeededFuture(responseBytes));
          }
        });
  }

  private static class DestroyableWebClient {
    private WebClient webClient;

    DestroyableWebClient(WebClient webClient) {
      this.webClient = webClient;
    }

    public void destroy() {
      webClient.close();
    }
  }
}
