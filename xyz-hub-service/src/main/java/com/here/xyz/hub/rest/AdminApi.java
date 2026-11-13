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

import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.createBodyHandler;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import com.here.xyz.hub.config.SpaceConfigClient.ClearResult;
import com.here.xyz.hub.connectors.RemoteFunctionClient;
import com.here.xyz.hub.connectors.statistics.StorageStatisticsProvider;
import com.here.xyz.hub.rest.ApiParam.Path;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.LazyTableClearing;
import com.here.xyz.models.hub.jwt.ActionMatrix;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class AdminApi extends Api {
  private static final Logger logger = LogManager.getLogger();

  public static final String MAIN_ADMIN_ENDPOINT = "/hub/admin/";
  public static final String ADMIN_MESSAGES_ENDPOINT = MAIN_ADMIN_ENDPOINT + "messages";
  public static final String ADMIN_STORAGE_STATISTICS = MAIN_ADMIN_ENDPOINT + "statistics/spaces/storage";
  public static final String ADMIN_METRICS_ENDPOINT = MAIN_ADMIN_ENDPOINT + "metrics";
  public static final String ADMIN_SPACE_LAZY_TABLE_ENDPOINT = MAIN_ADMIN_ENDPOINT + "spaces/:spaceId/lazyTable";

  private static final String ADMIN_CAPABILITY_MESSAGING = "messaging";
  public static final String ADMIN_CAPABILITY_STATISTICS = "statistics";
  public static final String ADMIN_CAPABILITY_LAZY_TABLE = "lazyTable";

  public static final String INCLUDE_CHANGES_SINCE = "includeChangesSince";
  static final String CONNECTOR = "connector";
  static final String TABLE = "table";
  static final String STORAGE_MISMATCH_MESSAGE = "Space storage does not match the given connector or table";

  public AdminApi(Vertx vertx, Router router, AuthenticationHandler auth) {
    router.route(HttpMethod.POST, ADMIN_MESSAGES_ENDPOINT)
        .handler(createBodyHandler())
        .handler(auth)
        .handler(this::onMessage);

    router.route(HttpMethod.GET, ADMIN_STORAGE_STATISTICS)
        .handler(auth)
        .handler(this::onStorageStatistics);

    router.route(HttpMethod.GET, ADMIN_METRICS_ENDPOINT)
        .handler(auth)
        .handler(this::onGetMetrics);

    router.route(HttpMethod.DELETE, ADMIN_SPACE_LAZY_TABLE_ENDPOINT)
        .handler(auth)
        .handler(this::onClearLazyTable);
  }

  /**
   * Removes the lazy-table flag of a space that was written without the Hub's feature write paths, e.g. by a job step.
   * The route trusts its caller to have written the space.
   */
  private void onClearLazyTable(final RoutingContext context) {
    try {
      AdminAuthorization.authorizeAdminCapability(context, ADMIN_CAPABILITY_LAZY_TABLE);
      String spaceId = context.pathParam(Path.SPACE_ID);
      String connector = Query.getString(context, CONNECTOR, null);
      String table = Query.getString(context, TABLE, null);
      LazyTableClearing.validateClearParams(spaceId, connector, table);
      Service.spaceConfigClient.clearLazyTable(getMarker(context), spaceId, connector, table, LazyTableClearing.SOURCE_ADMIN_API)
          .onFailure(t -> sendErrorResponse(context, t))
          .onSuccess(result -> {
            if (result == ClearResult.CONDITION_FAILED)
              sendErrorResponse(context, new HttpException(CONFLICT, STORAGE_MISMATCH_MESSAGE));
            else
              context.response().setStatusCode(NO_CONTENT.code()).end();
          });
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void onMessage(final RoutingContext context) {
    try {
      AdminAuthorization.authorizeAdminCapability(context, ADMIN_CAPABILITY_MESSAGING);
      Service.messageBroker.receiveRawMessage(context.getBody().getBytes());
      context
          .response()
          .setStatusCode(NO_CONTENT.code())
          .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
          .end();
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void onStorageStatistics(final RoutingContext context) {
    try {
      Marker marker = getMarker(context);
      AdminAuthorization.authorizeAdminCapability(context, ADMIN_CAPABILITY_STATISTICS);
      StorageStatisticsProvider.provideStorageStatistics(marker, Query.getLong(context, INCLUDE_CHANGES_SINCE, 0L))
          .onFailure(t -> sendErrorResponse(context, t))
          .onSuccess(storageStatistics -> context
              .response()
              .putHeader(CONTENT_TYPE, APPLICATION_JSON)
              .setStatusCode(OK.code())
              .setStatusMessage(OK.reasonPhrase())
              .end(storageStatistics.serialize()));
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void onGetMetrics(final RoutingContext context) {
    try {
      context
          .response()
          .putHeader(CONTENT_TYPE, TEXT_PLAIN)
          .setStatusCode(OK.code())
          .setStatusMessage(OK.reasonPhrase())
          .end(_buildPrometheusResponse());
    }
    catch (Exception e) {
      logger.error("Error creating Prometheus metric response", e);
      sendErrorResponse(context, e);
    }
  }

  private String _buildPrometheusResponse() {
    StringBuilder sb = new StringBuilder();

    //JVM memory metric
    float usedMemoryPercent = Service.getUsedMemoryPercent() / 100;
    sb.append(_buildPrometheusResponsePart("node_memory_usage_ratio",
        "The used memory percentage by the Java virtual machine", usedMemoryPercent));

    float globalUsedRfcConnections = RemoteFunctionClient.getGlobalUsedConnectionsPercentage();
    sb.append(_buildPrometheusResponsePart("GlobalUsedRfcConnections",
        "The utilized portion of RemoteFunctionClient connections pool", globalUsedRfcConnections));

    return sb.toString();
  }

  private String _buildPrometheusResponsePart(String name, String description, double value) {
     return "# HELP " + name + " " + description + "\n# TYPE " + name + " gauge\n" + name + "{hpa=\"true\"} " + value + "\n";
  }

  private static class AdminAuthorization extends Authorization {
    private static void authorizeAdminCapability(RoutingContext context, String capability) throws HttpException {
      final ActionMatrix tokenRights = getXyzHubMatrix(BaseHttpServerVerticle.getJWT(context));
      final XyzHubActionMatrix requestRights = new XyzHubActionMatrix()
          .useAdminCapabilities(XyzHubAttributeMap.forIdValues(capability));

      evaluateRights(getMarker(context), requestRights, tokenRights);
    }
  }
}
