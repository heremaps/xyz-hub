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
import com.here.xyz.hub.connectors.models.Connector.RemoteFunctionConfig.HTTP;
import com.here.xyz.hub.rest.HttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class HTTPFunctionClient extends QueueingRemoteFunctionClient {

  private static final Logger logger = LogManager.getLogger();

  private volatile WebClient webClient;
  private static volatile String url;

  public HTTPFunctionClient(Connector connectorConfig) {
    super(connectorConfig);
    if (!(connectorConfig.remoteFunction instanceof Connector.RemoteFunctionConfig.HTTP)) {
      throw new IllegalArgumentException("Invalid remoteFunctionConfig argument, must be an instance of HTTP");
    }

    updateStorageConfig();
  }

  @Override
  protected void updateStorageConfig() {
    super.updateStorageConfig();
    HTTP remoteFunctionConfig = (HTTP) connectorConfig.remoteFunction;
    url = remoteFunctionConfig.url.toString();
    webClient = WebClient.create(Service.vertx, new WebClientOptions()
        .setUserAgent(Service.XYZ_HUB_USER_AGENT)
        .setMaxPoolSize(getMaxConnections()));
  }

  @Override
  protected void invoke(Marker marker, byte[] bytes, Handler<AsyncResult<byte[]>> callback) {
    logger.debug(marker, "Invoke http remote function '{}' Event size is: {}", connectorConfig.remoteFunction.id, bytes.length);

    webClient.post(url)
        .timeout(REQUEST_TIMEOUT)
        .sendBuffer(Buffer.buffer(bytes), ar -> {
          if (ar.failed()) {
            if (ar.cause() instanceof TimeoutException) {
              callback.handle(Future.failedFuture(new HttpException(GATEWAY_TIMEOUT, "Connector timeout error.")));
            } else {
              callback.handle(Future.failedFuture(ar.cause()));
            }
          } else {
            try {
              //TODO: Refactor to move decompression into the base-class RemoteFunctionClient as it's not HTTP specific
              byte[] responseBytes = ar.result().body().getBytes();
              checkResponseSize(responseBytes);
              callback.handle(Future.succeededFuture(getDecompressed(responseBytes)));
            } catch (IOException | HttpException e) {
              callback.handle(Future.failedFuture(e));
            }
          }
        });
  }
}
