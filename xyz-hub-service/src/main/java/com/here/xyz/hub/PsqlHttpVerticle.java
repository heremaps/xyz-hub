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

package com.here.xyz.hub;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import com.here.xyz.hub.connectors.EmbeddedFunctionClient.EmbeddedContext;
import com.here.xyz.hub.rest.Api.Context;
import com.here.xyz.psql.PSQLXyzConnector;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PsqlHttpVerticle extends AbstractHttpServerVerticle {

  private static final Logger logger = LogManager.getLogger();
  private static Map<String, String> envMap;

  @Override
  public void start(Future future) {
    Router router = Router.router(vertx);
    router.route().handler(BodyHandler.create());
    router.route(HttpMethod.POST, "/psql").blockingHandler(PsqlHttpVerticle::connectorCall);
    addDefaultHandlers(router);
    vertx.createHttpServer(SERVER_OPTIONS)
        .requestHandler(router)
        .listen(config().getInteger("HTTP_PORT"), result -> {
          if (result.succeeded()) {
            future.complete();
          } else {
            logger.error("n error occurred, during the initialization of the server.", result.cause());
            future.fail(result.cause());
          }
        });
  }

  public static void connectorCall(RoutingContext context) {
    byte[] inputBytes = new byte[context.getBody().length()];
    context.getBody().getBytes(inputBytes);
    InputStream inputStream = new ByteArrayInputStream(inputBytes);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    EmbeddedContext embeddedContext = new EmbeddedContext(Context.getMarker(context), "psql", getEnvMap());
    new PSQLXyzConnector().handleRequest(inputStream, os, embeddedContext);
    context.response()
        .setStatusCode(HttpResponseStatus.OK.code())
        .setStatusMessage(HttpResponseStatus.OK.reasonPhrase())
        .putHeader(CONTENT_TYPE, APPLICATION_JSON)
        .end(Buffer.buffer(os.toByteArray()));
  }

  private static Map<String, String> getEnvMap() {
    if (envMap != null)
      return envMap;

    populateEnvMap();
    return envMap;
  }

  private static synchronized void populateEnvMap() {
    envMap = new HashMap<>();
    HttpConnector.configuration.fieldNames().forEach(fieldName -> {
      Object value = HttpConnector.configuration.getValue(fieldName);
      if (value != null) envMap.put(fieldName, value.toString());
    });
  }
}