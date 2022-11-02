/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.HttpConnector;
import com.here.xyz.hub.PsqlHttpVerticle;
import com.here.xyz.hub.connectors.EmbeddedFunctionClient;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.HttpConnectorTaskHandler;
import com.here.xyz.hub.util.health.MainHealthCheck;
import com.here.xyz.hub.util.health.schema.Reporter;
import com.here.xyz.hub.util.health.schema.Response;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;

import static com.here.xyz.hub.AbstractHttpServerVerticle.STREAM_INFO_CTX_KEY;
import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class HttpConnectorApi extends Api {

  private static final Logger logger = LogManager.getLogger();
  private AbstractConnectorHandler connector;

  public HttpConnectorApi(RouterBuilder rb, AbstractConnectorHandler connector) {
    this.connector = connector;
    this.connector.setEmbedded(true);
    rb.operation("postEvent").handler(this::postEvent);
    rb.operation("getHealthCheck").handler(this::getHealthCheck);

    rb.operation("getStatus").handler(this::getConnectorStatus);
    rb.operation("postInitialization").handler(this::postDatabaseInitialization);
    rb.operation("postMaintainIndices").handler(this::postMaintainIndices);

    rb.operation("getMaintenanceStatusSpace").handler(this::getMaintenanceStatusSpace);
    rb.operation("postMaintainSpace").handler(this::postMaintainSpace);
    rb.operation("postMaintainHistory").handler(this::postMaintainHistory);
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
            = new EmbeddedFunctionClient.EmbeddedContext(Context.getMarker(context), "psql", PsqlHttpVerticle.getEnvMap());
    connector.handleRequest(inputStream, os, embeddedContext, streamId);
    this.sendResponse(context, OK, os);
  }

  private void postDatabaseInitialization(final RoutingContext context) {
    String[] params = parseMainParams(context);
    final boolean force = Query.getBoolean(context, "force", false);

    try {
      HttpConnectorTaskHandler.initializeDatabase(params[0],params[1], params[2], force, ar -> {
        if (ar.failed()) {
          this.sendErrorResponse(context, ar.cause());
        }
        else {
          this.sendResponse(context, OK, ar.result());
        }
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void postMaintainIndices(final RoutingContext context) {
    String[] params = parseMainParams(context);
    final boolean autoIndexing = Query.getBoolean(context, "autoIndexing", false);

    try {
      HttpConnectorTaskHandler.maintainIndices(params[0],params[1], params[2], autoIndexing, ar -> {
        if (ar.failed()) {
          this.sendErrorResponse(context, ar.cause());
        }
        else {
          this.sendResponse(context, OK, ar.result());
        }
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void getConnectorStatus(final RoutingContext context) {
    String[] params = parseMainParams(context);

    try {
      HttpConnectorTaskHandler.getConnectorStatus(params[0],params[1], params[2], ar -> {
        if (ar.failed()) {
          this.sendErrorResponse(context, ar.cause());
        }
        else {
          this.sendResponse(context, OK, ar.result());
        }
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void postMaintainSpace(final RoutingContext context) {
    String[] params = parseMainParams(context);

    String spaceId = null;
    if (context.pathParam(ApiParam.Path.SPACE_ID) != null) {
      spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
    }
    final boolean force = Query.getBoolean(context, "force", false);

    try {
      HttpConnectorTaskHandler.maintainSpace(params[0],params[1], params[2],  spaceId, force, ar -> {
        if (ar.failed()) {
          this.sendErrorResponse(context, ar.cause());
        }
        else {
          this.sendResponse(context, OK, ar.result());
        }
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void getMaintenanceStatusSpace(final RoutingContext context) {
    String[] params = parseMainParams(context);

    String spaceId = null;
    if (context.pathParam(ApiParam.Path.SPACE_ID) != null) {
      spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
    }

    try {
      HttpConnectorTaskHandler.getMaintenanceStatusOfSpace(params[0],params[1], params[2],  spaceId, ar -> {
        if (ar.failed()) {
          this.sendErrorResponse(context, ar.cause());
        }
        else {
          this.sendResponse(context, OK, ar.result());
        }
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void postMaintainHistory(final RoutingContext context) {
    String[] params = parseMainParams(context);
    final int maxVersionCount = Query.getInteger(context, "maxVersionCount", -1);
    final int currentVersion = Query.getInteger(context, "currentVersion", -1);

    String spaceId = null;
    if (context.pathParam(ApiParam.Path.SPACE_ID) != null) {
      spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
    }

    try {
      HttpConnectorTaskHandler.maintainHistory(params[0],params[1], params[2], spaceId, currentVersion, maxVersionCount, ar -> {
        if (ar.failed()) {
          sendErrorResponse(context, ar.cause());
        }
        else {
          sendResponse(context, OK, ar.result());
        }
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private String[] parseMainParams(RoutingContext context) {
    String connectorId = Query.getString(context, "connectorId", null);
    addStreamInfo(context, "SID", connectorId);

    return new String[]{
            connectorId,
            Query.getString(context, "ecps", null),
            Query.getString(context, "passphrase", HttpConnector.configuration.ECPS_PHRASE)
    };
  }

  private static void addStreamInfo(final RoutingContext context, String key, Object value){
    context.put(STREAM_INFO_CTX_KEY, new HashMap<String, Object>(){{put(key, value);}});
  }
}
