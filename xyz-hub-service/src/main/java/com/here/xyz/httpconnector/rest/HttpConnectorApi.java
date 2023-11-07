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

package com.here.xyz.httpconnector.rest;

import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.httpconnector.PsqlHttpConnectorVerticle;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.connectors.EmbeddedFunctionClient;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.util.health.MainHealthCheck;
import com.here.xyz.hub.util.health.schema.Reporter;
import com.here.xyz.hub.util.health.schema.Response;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;

public class HttpConnectorApi extends Api {
  private AbstractConnectorHandler connector;

  public HttpConnectorApi(RouterBuilder rb, AbstractConnectorHandler connector) {
    this.connector = connector;
    this.connector.setEmbedded(true);
    rb.operation("postEvent").handler(this::postEvent);
    rb.operation("getHealthCheck").handler(this::getHealthCheck);
  }

  private void getHealthCheck(final RoutingContext context) {
    MainHealthCheck hc = new MainHealthCheck(false)
        .withReporter(
                new Reporter()
                        .withVersion(Core.BUILD_VERSION)
                        .withName("HERE HTTP-Connector")
                        .withBuildDate(Core.BUILD_TIME)
                        .withUpSince(Core.START_TIME)
        );
    Response r = hc.getResponse();
    sendResponse(context, OK, r);
  }

  private void postEvent(final RoutingContext context) {
    String streamId = Context.getMarker(context).getName();
    byte[] inputBytes = new byte[context.getBody().length()];
    context.getBody().getBytes(inputBytes);
    InputStream inputStream = new ByteArrayInputStream(inputBytes);
    ByteArrayOutputStream os = new ByteArrayOutputStream();
    EmbeddedFunctionClient.EmbeddedContext embeddedContext
            = new EmbeddedFunctionClient.EmbeddedContext(Context.getMarker(context), "psql", PsqlHttpConnectorVerticle.getEnvMap());
    connector.handleRequest(inputStream, os, embeddedContext, streamId);
    this.sendResponse(context, OK, os);
  }

}
