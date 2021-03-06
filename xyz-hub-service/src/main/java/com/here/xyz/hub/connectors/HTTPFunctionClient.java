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

import static com.google.common.net.HttpHeaders.ACCEPT_ENCODING;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpResponseStatus.TOO_MANY_REQUESTS;

import com.google.common.base.Strings;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig;
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.Http;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.LimitedOffHeapQueue.PayloadVanishedException;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.impl.ConnectionBase;
import io.vertx.ext.web.client.HttpResponse;
import java.nio.charset.Charset;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class HTTPFunctionClient extends RemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();
  private volatile String url;

  HTTPFunctionClient(Connector connectorConfig) {
    super(connectorConfig);
  }

  @Override
  synchronized void setConnectorConfig(final Connector newConnectorConfig) throws NullPointerException, IllegalArgumentException {
    super.setConnectorConfig(newConnectorConfig);
    if (!(getConnectorConfig().getRemoteFunction() instanceof Http)) {
      throw new IllegalArgumentException("Invalid remoteFunctionConfig argument, must be an instance of HTTP");
    }
    url = ((Http) getConnectorConfig().getRemoteFunction()).url.toString();
  }

  @Override
  void destroy() {
    super.destroy();
  }

  protected void invoke(FunctionCall fc, Handler<AsyncResult<byte[]>> callback) {
    final RemoteFunctionConfig remoteFunction = getConnectorConfig().getRemoteFunction();
    logger.info(fc.marker, "Invoke http remote function '{}' URL is: {} Event size is: {}",
        remoteFunction.id, url, fc.getByteSize());

    try {
      Service.webClient.postAbs(url)
          .timeout(REQUEST_TIMEOUT)
          .putHeader(CONTENT_TYPE, "application/json; charset=" + Charset.defaultCharset().name())
          .putHeader(STREAM_ID, fc.marker.getName())
          .putHeader(ACCEPT_ENCODING, "gzip")
          .sendBuffer(Buffer.buffer(fc.consumePayload()), ar -> {
            if (fc.fireAndForget) return;
            if (ar.failed())
              handleFailure(fc.marker, callback, ar.cause());
            else {
              try {
                validateHttpStatus(ar.result());
                byte[] responseBytes = ar.result().body().getBytes();
                callback.handle(Future.succeededFuture(responseBytes));
              }
              catch (Exception e) {
                handleFailure(fc.marker, callback, new HttpException(BAD_GATEWAY, "Error while handling response of HTTP connector.", e));
              }
            }
          });
    }
    catch (PayloadVanishedException e) {
      callback.handle(Future.failedFuture(new HttpException(TOO_MANY_REQUESTS, "Remote function is busy or cannot be invoked.")));
    }
    catch (Exception e) {
      handleFailure(fc.marker, callback, e);
    }
  }

  private void validateHttpStatus(HttpResponse response) throws HttpException {
    if (response.statusCode() != OK.code()) {
      HttpResponseStatus upstreamStatus = Strings.isNullOrEmpty(response.statusMessage()) ?
          HttpResponseStatus.valueOf(response.statusCode()) : new HttpResponseStatus(response.statusCode(), response.statusMessage());
      HttpException upstreamHttpEx = new HttpException(upstreamStatus, "Remote HTTP connector service responded with: " + upstreamStatus);
      if (upstreamStatus.equals(GATEWAY_TIMEOUT))
        throw upstreamHttpEx;
      throw new HttpException(BAD_GATEWAY, "Remote HTTP connector service did not respond with 200(OK)", upstreamHttpEx);
    }
    else if (response.body() == null)
      throw new HttpException(BAD_GATEWAY, "Response body from remote HTTP connector service was empty.");
  }

  private void handleFailure(Marker marker, Handler<AsyncResult<byte[]>> callback, Throwable t) {
    if (t == ConnectionBase.CLOSED_EXCEPTION)
      //Re-attach a stack-trace until here
      t = new RuntimeException("Connection was already closed.", t);
    logger.warn(marker, "Error while calling remote HTTP service", t);
    if (t instanceof TimeoutException)
      t = new HttpException(GATEWAY_TIMEOUT, "Connector timeout error.", t);
    if (!(t instanceof HttpException))
      t = new HttpException(BAD_GATEWAY, "Connector error.", t);
    callback.handle(Future.failedFuture(t));
  }
}
