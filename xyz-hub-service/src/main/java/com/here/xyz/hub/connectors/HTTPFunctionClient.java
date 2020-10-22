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
    if (!(getConnectorConfig().remoteFunction instanceof Http)) {
      throw new IllegalArgumentException("Invalid remoteFunctionConfig argument, must be an instance of HTTP");
    }
    url = ((Http) getConnectorConfig().remoteFunction).url.toString();
  }

  @Override
  void destroy() {
    super.destroy();
  }

  protected void invoke(FunctionCall fc, Handler<AsyncResult<byte[]>> callback) {
    invokeWithRetry(fc, 0, callback);
  }

  private void invokeWithRetry(FunctionCall fc, int tryCount, Handler<AsyncResult<byte[]>> callback) {
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().remoteFunction;
    logger.info(fc.marker, "Invoke http remote function '{}' URL is: {} Event size is: {}",
        remoteFunction.id, url, fc.getByteSize());

    final int nextTryCount = tryCount + 1;
    try {
      Service.webClient.postAbs(url)
          .timeout(REQUEST_TIMEOUT)
          .putHeader(CONTENT_TYPE, "application/json; charset=" + Charset.defaultCharset().name())
          .putHeader(STREAM_ID, fc.marker.getName())
          .sendBuffer(Buffer.buffer(fc.getPayload()), ar -> {
            if (fc.fireAndForget) return;
            if (ar.failed()) {
              if (ar.cause() instanceof TimeoutException) {
                callback.handle(Future.failedFuture(new HttpException(GATEWAY_TIMEOUT, "Connector timeout error.")));
              }
              else {
                handleFailure(fc, nextTryCount, callback, new RuntimeException(ar.cause()));
              }
            }
            else {
              try {
                byte[] responseBytes = ar.result().body().getBytes();
                callback.handle(Future.succeededFuture(responseBytes));
              }
              catch (Exception e) {
                handleFailure(fc, nextTryCount, callback, ConnectionBase.CLOSED_EXCEPTION);
              }
            }
          });
    }
    catch (Exception e) {
      handleFailure(fc, nextTryCount, callback, e);
    }
  }

  private void handleFailure(FunctionCall fc, int tryCount, Handler<AsyncResult<byte[]>> callback, Exception e) {
    if (e == ConnectionBase.CLOSED_EXCEPTION) {
      e = new RuntimeException("Connection was already closed.", e);
      if (tryCount <= 1) {
        logger.warn(fc.marker, e.getMessage() + " Retrying ...", e);
        invokeWithRetry(fc, tryCount, callback);
        return;
      }
    }
    logger.warn(fc.marker, "Error sending event to remote http service", e);
    callback.handle(Future.failedFuture(new HttpException(BAD_GATEWAY, "Connector error.", e)));
  }
}
