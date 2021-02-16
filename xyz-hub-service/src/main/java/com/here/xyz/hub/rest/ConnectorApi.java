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

package com.here.xyz.hub.rest;

import com.here.xyz.hub.auth.*;
import com.here.xyz.hub.rest.admin.MessageBroker;
import com.here.xyz.hub.task.ConnectorHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.charset.StandardCharsets;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.ApiParam.Query.OWNER;
import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

public class ConnectorApi extends Api {
  private static final Logger logger = LogManager.getLogger();

  public static final String CONNECTOR_ENDPOINT = "/hub/connectors";
  public static final String CONNECTOR_ENDPOINT_WITH_ID = "/hub/connectors/:connectorId";
  private static final MessageBroker messageBroker = MessageBroker.getInstance();

  public ConnectorApi(Vertx vertx, Router router, AuthHandler auth) {
    router.route(HttpMethod.GET, CONNECTOR_ENDPOINT)
        .handler(auth)
        .produces(APPLICATION_JSON)
        .handler(this::getConnectors);
    router.route(HttpMethod.GET, CONNECTOR_ENDPOINT_WITH_ID)
        .handler(auth)
        .produces(APPLICATION_JSON)
        .handler(this::getConnector);

    router.route(HttpMethod.POST, CONNECTOR_ENDPOINT)
        .consumes(APPLICATION_JSON)
        .handler(auth)
        .produces(APPLICATION_JSON)
        .handler(this::createConnector);
    router.route(HttpMethod.PUT, CONNECTOR_ENDPOINT_WITH_ID)
        .consumes(APPLICATION_JSON)
        .handler(auth)
        .produces(APPLICATION_JSON)
        .handler(this::replaceConnector);
    router.route(HttpMethod.PATCH, CONNECTOR_ENDPOINT_WITH_ID)
        .consumes(APPLICATION_JSON)
        .handler(auth)
        .produces(APPLICATION_JSON)
        .handler(this::updateConnector);
    router.route(HttpMethod.DELETE, CONNECTOR_ENDPOINT_WITH_ID)
        .handler(auth)
        .produces(APPLICATION_JSON)
        .handler(this::deleteConnector);
  }

  private void getConnector(final RoutingContext context) {

    try {
      ConnectorAuthorization.authorizeManageConnectorsRights(context);
    } catch (Exception e) {
      sendErrorResponse(context, e);
      return;
    }

    String connectorId = context.pathParam(ApiParam.Path.CONNECTOR_ID);
    ConnectorHandler.getConnector(context, connectorId, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          } else {
            sendResponse(context, OK, ar.result());
          }
        }
    );

  }

  private void getConnectors(final RoutingContext context) {
    try {
      ConnectorAuthorization.authorizeManageConnectorsRights(context);
    } catch (Exception e) {
      sendErrorResponse(context, e);
      return;
    }

    ConnectorHandler.getConnectors(context, Context.getJWT(context).aid, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          } else {
            sendResponse(context, OK, ar.result());
          }
        }
    );
  }

  private void createConnector(final RoutingContext context) {
    try {
      ConnectorAuthorization.authorizeManageConnectorsRights(context);
    } catch (Exception e) {
      sendErrorResponse(context, e);
      return;
    }

    JsonObject input;
    try {
      input = context.getBodyAsJson();
    } catch (DecodeException e) {
      context.fail(new HttpException(BAD_REQUEST, "Invalid JSON string"));
      return;
    }

    ConnectorHandler.createConnector(context, input, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          } else {
            sendResponse(context, CREATED, ar.result());
          }
        }
    );
  }

  private void replaceConnector(final RoutingContext context) {
    try {
      ConnectorAuthorization.authorizeManageConnectorsRights(context);
    } catch (Exception e) {
      sendErrorResponse(context, e);
      return;
    }

    JsonObject input;
    try {
      input = context.getBodyAsJson();
    } catch (DecodeException e) {
      context.fail(new HttpException(BAD_REQUEST, "Invalid JSON string"));
      return;
    }

    String connectorId = context.pathParam(ApiParam.Path.CONNECTOR_ID);
    if (input.getString("id") == null) {
      input.put("id", connectorId);
    } else if (!input.getString("id").equals(connectorId)) {
      sendErrorResponse(context, new HttpException(BAD_REQUEST, "Path ID does not match resource ID in body."));
      return;
    }

    ConnectorHandler.replaceConnector(context, input, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          } else {
            sendResponse(context, OK, ar.result());
          }
        }
    );
  }

  private void updateConnector(final RoutingContext context) {
    try {
      ConnectorAuthorization.authorizeManageConnectorsRights(context);
    } catch (Exception e) {
      sendErrorResponse(context, e);
      return;
    }

    JsonObject input;
    try {
      input = context.getBodyAsJson();
    } catch (DecodeException e) {
      context.fail(new HttpException(BAD_REQUEST, "Invalid JSON string"));
      return;
    }

    String connectorId = context.pathParam(ApiParam.Path.CONNECTOR_ID);
    if (input.getString("id") == null) {
      input.put("id", connectorId);
    } else if (!input.getString("id").equals(connectorId)) {
      sendErrorResponse(context, new HttpException(BAD_REQUEST, "Path ID does not match resource ID in body."));
      return;
    }

    ConnectorHandler.updateConnector(context, input, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          } else {
            sendResponse(context, OK, ar.result());
          }
        }
    );
  }

  private void deleteConnector(final RoutingContext context) {
    try {
      ConnectorAuthorization.authorizeManageConnectorsRights(context);
    } catch (Exception e) {
      sendErrorResponse(context, e);
      return;
    }

    String connectorId = context.pathParam(ApiParam.Path.CONNECTOR_ID);
    ConnectorHandler.deleteConnector(context, connectorId, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          } else {
            sendResponse(context, OK, ar.result());
          }
        }
    );
  }

  private void sendResponse(RoutingContext context, HttpResponseStatus status, Object o) {
    HttpServerResponse httpResponse = context.response().setStatusCode(status.code());

    byte[] response;
    try {
      response = Json.encode(o).getBytes(StandardCharsets.UTF_8);
    } catch (EncodeException e) {
      sendErrorResponse(context, new HttpException(INTERNAL_SERVER_ERROR, "Could not serialize response.", e));
      return;
    }

    if (response == null || response.length == 0) {
      httpResponse.setStatusCode(NO_CONTENT.code()).end();
    } else if (response.length > getMaxResponseLength(context)) {
      sendErrorResponse(context, new HttpException(RESPONSE_PAYLOAD_TOO_LARGE, RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE));
    } else {
      httpResponse.putHeader(CONTENT_TYPE, APPLICATION_JSON);
      httpResponse.end(Buffer.buffer(response));
    }
  }

  private void sendErrorResponse(RoutingContext context, Throwable throwable) {
    HttpException e;
    if (throwable instanceof HttpException) {
      e = (HttpException) throwable;
    } else {
      e = new HttpException(INTERNAL_SERVER_ERROR, throwable.getMessage(), throwable);
    }
    sendErrorResponse(context, e);
  }


  private static class ConnectorAuthorization extends Authorization {
    public static void authorizeManageConnectorsRights(RoutingContext context) throws HttpException {
      JWTPayload jwt = Context.getJWT(context);

      XyzHubAttributeMap attributeMap = new XyzHubAttributeMap();
      attributeMap.withValue(OWNER, jwt.aid);
      if(context.pathParam(ApiParam.Path.CONNECTOR_ID) != null && !context.pathParam(ApiParam.Path.CONNECTOR_ID).isEmpty())
        attributeMap.withValue("id", context.pathParam(ApiParam.Path.CONNECTOR_ID));

      final ActionMatrix tokenRights = jwt.getXyzHubMatrix();
      final XyzHubActionMatrix requestRights = new XyzHubActionMatrix()
          .manageConnectors(attributeMap);

      evaluateRights(Context.getMarker(context), requestRights, tokenRights);
    }
  }
}
