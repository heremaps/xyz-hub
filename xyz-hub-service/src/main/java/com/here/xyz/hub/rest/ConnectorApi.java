/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.util.service.BaseHttpServerVerticle.getJWT;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.FORBIDDEN;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.task.ConnectorHandler;
import com.here.xyz.hub.util.diff.Difference;
import com.here.xyz.models.hub.jwt.AttributeMap;
import com.here.xyz.util.service.HttpException;
import com.here.xyz.util.service.errors.DetailedHttpException;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConnectorApi extends Api {
  private static final Logger logger = LogManager.getLogger();

  public ConnectorApi(RouterBuilder rb) {
    rb.getRoute("getConnectors").setDoValidation(false).addHandler(this::getConnectors);
    rb.getRoute("postConnector").setDoValidation(false).addHandler(this::createConnector);
    rb.getRoute("getConnector").setDoValidation(false).addHandler(this::getConnector);
    rb.getRoute("patchConnector").setDoValidation(false).addHandler(this::updateConnector);
    rb.getRoute("deleteConnector").setDoValidation(false).addHandler(this::deleteConnector);
  }

  private JsonObject getInput(final RoutingContext context) throws HttpException {
    try {
      return context.body().asJsonObject();
    }
    catch (DecodeException e) {
      throw new DetailedHttpException("E318401", e);
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
            this.sendResponse(context, OK, ar.result());
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
        if (ConnectorAuthorization.isAdmin(context)) {
          ConnectorHandler.getAllConnectors(context, ar -> {
                if (ar.failed()) {
                  sendErrorResponse(context, ar.cause());
                } else {
                  sendResponse(context, OK, ar.result());
                }
              }
          );
        } else {
          ConnectorHandler.getConnectors(context, getJWT(context).aid, ar -> {
                if (ar.failed()) {
                  sendErrorResponse(context, ar.cause());
                } else {
                  sendResponse(context, OK, ar.result());
                }
              }
          );
        }
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

  public static class ConnectorAuthorization extends Authorization {
    public static void authorizeManageConnectorsRights(RoutingContext context, String connectorId, Handler<AsyncResult<Void>> handler) {
      authorizeManageConnectorsRights(context, Collections.singletonList(connectorId), handler);
    }

    public static void authorizeManageConnectorsRights(RoutingContext context, List<String> connectorIds, Handler<AsyncResult<Void>> handler) {
      final XyzHubActionMatrix requestRights = new XyzHubActionMatrix();
      List<CompletableFuture<Void>> futureList = connectorIds == null ? Collections.emptyList()
          : connectorIds.stream().map(connectorId -> checkConnector(context, requestRights, connectorId)).collect(Collectors.toList());

      CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
          .thenRun(() -> {
            try {
              evaluateRights(getMarker(context), requestRights, getXyzHubMatrix(getJWT(context)));
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
          requestRights.manageConnectors(XyzHubAttributeMap.forIdValues(getJWT(context).aid, connectorId));
          f.complete(null);
        }
      });
      return f;
    }

    public static void validateAdminChanges(RoutingContext context, Difference.DiffMap diffMap) throws HttpException {
      //Is Admin change?
      if (!isAdmin(context)) {
        checkParameterChange(diffMap.get("owner"), "owner", getJWT(context).aid);
        checkParameterChange(diffMap.get("skipAutoDisable"), "skipAutoDisable", false);
        checkParameterChange(diffMap.get("trusted"), "trusted", false);
      }
    }

    private static boolean isAdmin(RoutingContext context) {
      XyzHubActionMatrix xyzHubActionMatrix = getXyzHubMatrix(getJWT(context));
      if (xyzHubActionMatrix == null) return false;
      List<AttributeMap> manageConnectorsRights = xyzHubActionMatrix.get(XyzHubActionMatrix.MANAGE_CONNECTORS);
      if (manageConnectorsRights != null) {
        for (AttributeMap attributeMap : manageConnectorsRights) {
          //If manageConnectors[{}] -> Admin (no restrictions)
          if (attributeMap.isEmpty()) {
            return true;
          }
        }
      }
      return false;
    }

    private static void checkParameterChange(Difference diff, String parameterName, Object expected) throws HttpException {
      if (diff instanceof Difference.Primitive) {
        Difference.Primitive prim = (Difference.Primitive) diff;
        if (prim.newValue() != null && !prim.newValue().equals(expected)) throw new HttpException(FORBIDDEN, "The property '" + parameterName + "' can not be changed manually.");
      }
    }
  }
}
