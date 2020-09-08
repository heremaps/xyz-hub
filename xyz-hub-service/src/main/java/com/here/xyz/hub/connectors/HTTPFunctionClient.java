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

import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
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
import io.vertx.core.net.impl.ConnectionBase;
import io.vertx.ext.web.client.WebClient;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class HTTPFunctionClient extends RemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();
  private volatile String url;

  HTTPFunctionClient(Connector connectorConfig) {
    super(connectorConfig);
  }

  @Override
  synchronized void setConnectorConfig(final Connector newConnectorConfig) throws NullPointerException, IllegalArgumentException {
    super.setConnectorConfig(newConnectorConfig);
    createClient();
  }

  private void createClient() {
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().remoteFunction;
    if (!(remoteFunction instanceof Http)) {
      throw new IllegalArgumentException("Invalid remoteFunctionConfig argument, must be an instance of HTTP");
    }
    Http httpRemoteFunction = (Http) remoteFunction;
    url = httpRemoteFunction.url.toString();
  }

  @Override
  void destroy() {
    super.destroy();
  }

  private static void shutdownWebClient(WebClient webClient) {
    if (webClient == null) {
      return;
    }
    //Shutdown the web client after the request timeout
    //TODO: Use CompletableFuture.delayedExecutor() after switching to Java 9
    new Thread(() -> {
      try {
        Thread.sleep(REQUEST_TIMEOUT);
      } catch (InterruptedException ignored) {
      }
      webClient.close();
    }).start();
  }

  @Override
  protected void invoke(FunctionCall fc, Handler<AsyncResult<byte[]>> callback) {
    //TODO: respect fc.fireAndForget parameter
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().remoteFunction;
    logger.info(fc.marker, "Invoke http remote function '{}' URL is: {} Event size is: {}", remoteFunction.id, url, fc.bytes.length);

    int tryCount = 0;
    boolean retry;
    do {
      retry = false;
      tryCount++;
      try {
        Service.webClient.postAbs(url)
            .timeout(REQUEST_TIMEOUT)
            .putHeader(CONTENT_TYPE, "application/json; charset=" + Charset.defaultCharset().name())
            .putHeader(STREAM_ID, fc.marker.getName())
            .sendBuffer(Buffer.buffer(fc.bytes), ar -> {
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
      } catch (Exception e) {
        if (e == ConnectionBase.CLOSED_EXCEPTION) {
          e = new RuntimeException("Connection was already closed.", e);
          if (tryCount <= 1) {
            retry = true;
          }
          logger.error(e.getMessage() + (retry ? " Retrying ..." : ""), e);
        }
        if (!retry) {
          logger.error(fc.marker, "Error sending event to remote http service", e);
          callback.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Connector error.", e)));
        }
      }
    } while (retry && tryCount <= 1);
  }
}
