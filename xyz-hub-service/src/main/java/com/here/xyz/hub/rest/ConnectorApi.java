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

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.task.ConnectorHandler;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.EncodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectorApi extends Api {
  private static final Logger logger = LogManager.getLogger();

  public ConnectorApi(OpenAPI3RouterFactory routerFactory) {
    routerFactory.addHandlerByOperationId("getConnectors", this::getConnectors);
    routerFactory.addHandlerByOperationId("postConnector", this::createConnector);
    routerFactory.addHandlerByOperationId("getConnector", this::getConnector);
    routerFactory.addHandlerByOperationId("putConnector", this::replaceConnector);
    routerFactory.addHandlerByOperationId("patchConnector", this::updateConnector);
    routerFactory.addHandlerByOperationId("deleteConnector", this::deleteConnector);
  }

  private JsonObject getInput(final RoutingContext context) throws HttpException {
    try {
      return context.getBodyAsJson();
    }
    catch (DecodeException e) {
      throw new HttpException(BAD_REQUEST, "Invalid JSON string");
    }
  }

  private void getConnector(final RoutingContext context) {
    try {
      String connectorId = context.pathParam(ApiParam.Path.CONNECTOR_ID);
      ConnectorAuthorization.authorizeManageConnectorsRights(context, connectorId, arAuth -> {
        if (arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
          return;
        }
        ConnectorHandler.getConnector(context, connectorId, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          }
          else {
            sendResponse(context, OK, ar.result());
          }
        });
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void getConnectors(final RoutingContext context) {
    List<String> queryIds = context.queryParam("id");
    Handler<AsyncResult<Void>> handler = arAuth -> {
      if (arAuth.failed()) {
        sendErrorResponse(context, arAuth.cause());
        return;
      }

      if (queryIds.isEmpty()) {
        ConnectorHandler.getConnectors(context, Context.getJWT(context).aid, ar -> {
              if (ar.failed()) {
                sendErrorResponse(context, ar.cause());
              } else {
                sendResponse(context, OK, ar.result());
              }
            }
        );
      } else {
        ConnectorHandler.getConnectors(context, queryIds, ar -> {
              if (ar.failed()) {
                sendErrorResponse(context, ar.cause());
              } else {
                sendResponse(context, OK, ar.result());
              }
            }
        );
      }
    };

    ConnectorAuthorization.authorizeManageConnectorsRights(context, queryIds, handler);
  }

  private void createConnector(final RoutingContext context) {
    try {
      JsonObject input = getInput(context);
      String connectorId = input.getString("id");
      if (connectorId == null)
        throw new HttpException(BAD_REQUEST, "Parameter 'id' for the resource is missing.");
      ConnectorAuthorization.authorizeManageConnectorsRights(context, connectorId, arAuth -> {
        if (arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
          return;
        }
        ConnectorHandler.createConnector(context, input, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          }
          else {
            sendResponse(context, CREATED, ar.result());
          }
        });
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void validateConnectorId(RoutingContext context, JsonObject input) throws HttpException {
    String connectorId = context.pathParam(ApiParam.Path.CONNECTOR_ID);
    if (input.getString("id") == null) {
      input.put("id", connectorId);
    }
    else if (!input.getString("id").equals(connectorId)) {
      throw new HttpException(BAD_REQUEST, "Path ID does not match resource ID in body.");
    }
  }

  private void replaceConnector(final RoutingContext context) {
    try {
      String connectorId = context.pathParam(ApiParam.Path.CONNECTOR_ID);
      JsonObject input = getInput(context);
      validateConnectorId(context, input);
      ConnectorAuthorization.authorizeManageConnectorsRights(context, connectorId, arAuth -> {
        if (arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
          return;
        }
        ConnectorHandler.replaceConnector(context, input, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          }
          else {
            sendResponse(context, OK, ar.result());
          }
        });
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void updateConnector(final RoutingContext context) {
    try {
      String connectorId = context.pathParam(ApiParam.Path.CONNECTOR_ID);
      JsonObject input = getInput(context);
      validateConnectorId(context, input);
      ConnectorAuthorization.authorizeManageConnectorsRights(context, connectorId, arAuth -> {
        if (arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
          return;
        }
        ConnectorHandler.updateConnector(context, input, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          }
          else {
            sendResponse(context, OK, ar.result());
          }
        });
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void deleteConnector(final RoutingContext context) {
    try {
      String connectorId = context.pathParam(ApiParam.Path.CONNECTOR_ID);
      ConnectorAuthorization.authorizeManageConnectorsRights(context, connectorId, arAuth -> {
        if (arAuth.failed()) {
          sendErrorResponse(context, arAuth.cause());
          return;
        }
        ConnectorHandler.deleteConnector(context, connectorId, ar -> {
          if (ar.failed()) {
            this.sendErrorResponse(context, ar.cause());
          }
          else {
            sendResponse(context, OK, ar.result());
          }
        });
      });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void sendResponse(RoutingContext context, HttpResponseStatus status, Object o) {
    HttpServerResponse httpResponse = context.response().setStatusCode(status.code());

    byte[] response;
    try {
      response = Json.encode(o).getBytes();
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
    super.sendErrorResponse(context, throwable instanceof Exception ? (Exception) throwable : new Exception(throwable));
  }


  private static class ConnectorAuthorization extends Authorization {
    public static void authorizeManageConnectorsRights(RoutingContext context, String connectorId, Handler<AsyncResult<Void>> handler) {
      authorizeManageConnectorsRights(context, Arrays.asList(connectorId), handler);
    }
      
    public static void authorizeManageConnectorsRights(RoutingContext context, List<String> connectorIds, Handler<AsyncResult<Void>> handler) {
      final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();
      List<CompletableFuture<Void>> futureList = connectorIds == null ? Collections.emptyList()
          : connectorIds.stream().map(connectorId -> checkConnector(context, requestRights, connectorId)).collect(Collectors.toList());

      CompletableFuture.allOf(futureList.toArray(new CompletableFuture[futureList.size()]))
          .thenRun(() -> {
            try {
              evaluateRights(Context.getMarker(context), requestRights, Context.getJWT(context).getXyzHubMatrix());
              handler.handle(Future.succeededFuture());
            } catch (HttpException e) {
              handler.handle(Future.failedFuture(e));
            }
          });
    }

    private static CompletableFuture<Void> checkConnector(RoutingContext context, XyzHubActionMatrix requestRights, String connectorId) {
      CompletableFuture<Void> f = new CompletableFuture<>();
      ConnectorHandler.getConnector(context, connectorId, ar -> {
        if (ar.succeeded()) {
          Connector c = ar.result();
          if (c.owner != null)
            requestRights.manageConnectors(XyzHubAttributeMap.forIdValues(c.owner, c.id));
          else
            requestRights.manageConnectors(XyzHubAttributeMap.forIdValues(c.id));
          f.complete(null);
        } else {
          //If connector does not exist.
          requestRights.manageConnectors(XyzHubAttributeMap.forIdValues(Context.getJWT(context).aid, connectorId));
          f.complete(null);
        }
      });
      return f;
    }
  }
}
