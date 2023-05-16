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

import static io.netty.handler.codec.http.HttpResponseStatus.*;

import com.here.xyz.hub.auth.AttributeMap;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import com.here.xyz.hub.events.GetConnectorsByIdEvent;
import com.here.xyz.hub.task.connector.ConnectorHandler;
import com.here.xyz.hub.task.NakshaTask;
import com.here.xyz.util.diff.Difference;
import com.here.xyz.util.diff.MapDiff;
import com.here.xyz.util.diff.PrimitiveDiff;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class ConnectorApi extends Api {

  public ConnectorApi(RouterBuilder rb) {
    rb.operation("getConnectors").handler(this::getConnectors);
    rb.operation("postConnector").handler(this::createConnector);
    rb.operation("getConnector").handler(this::getConnector);
    rb.operation("patchConnector").handler(this::updateConnector);
    rb.operation("deleteConnector").handler(this::deleteConnector);
  }

  private void getConnector(final RoutingContext routingContext) {
    NakshaTask.start(GetConnectorsByIdEvent.class, routingContext, ApiResponseType.FEATURE);
  }

  private void getConnectors(final RoutingContext routingContext) {
    NakshaTask.start(GetConnectorsByIdEvent.class, routingContext, ApiResponseType.FEATURE_COLLECTION);
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
      if (connectorIds==null || connectorIds.isEmpty()) {
        requestRights.manageConnectors(new XyzHubAttributeMap()); // "manageConnectors" access required for <all> connectors
      }
      CompletableFuture.allOf(futureList.toArray(new CompletableFuture[0]))
          .thenRun(() -> {
            try {
              evaluateRights(Context.getMarker(context), requestRights, Context.jwt(context).getXyzHubMatrix());
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
          requestRights.manageConnectors(XyzHubAttributeMap.forIdValues(Context.jwt(context).aid, connectorId));
          f.complete(null);
        }
      });
      return f;
    }

    public static void validateAdminChanges(RoutingContext context, MapDiff diffMap) throws HttpException {
      //Is Admin change?
      if (!isAdmin(context)) {
        checkParameterChange(diffMap.get("owner"), "owner", Context.jwt(context).aid);
        checkParameterChange(diffMap.get("skipAutoDisable"), "skipAutoDisable", false);
        checkParameterChange(diffMap.get("trusted"), "trusted", false);
      }
    }

    private static boolean isAdmin(RoutingContext context) {
      XyzHubActionMatrix xyzHubActionMatrix = Context.jwt(context).getXyzHubMatrix();
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
      if (diff instanceof PrimitiveDiff) {
        PrimitiveDiff prim = (PrimitiveDiff) diff;
        if (prim.newValue() != null && !prim.newValue().equals(expected)) throw new HttpException(FORBIDDEN, "The property '" + parameterName + "' can not be changed manually.");
      }
    }
  }
}
