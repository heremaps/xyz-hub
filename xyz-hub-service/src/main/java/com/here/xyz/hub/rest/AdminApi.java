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

package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.ApiParam.Query.SKIP_CACHE;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.ActionMatrix;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import com.here.xyz.hub.connectors.RemoteFunctionClient;
import com.here.xyz.hub.connectors.statistics.StorageStatisticsProvider;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.FeatureTask;
import com.here.xyz.hub.task.FeatureTask.GeometryQuery;
import com.here.xyz.hub.task.FeatureTask.IdsQuery;
import com.here.xyz.hub.task.FeatureTask.IterateQuery;
import com.here.xyz.hub.task.FeatureTask.LoadFeaturesQuery;
import com.here.xyz.hub.task.FeatureTask.SearchQuery;
import com.here.xyz.hub.task.FeatureTask.TileQuery;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.BodyHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class AdminApi extends Api {
  private static final Logger logger = LogManager.getLogger();

  public static final String MAIN_ADMIN_ENDPOINT = "/hub/admin/";
  public static final String ADMIN_MESSAGES_ENDPOINT = MAIN_ADMIN_ENDPOINT + "messages";
  public static final String ADMIN_EVENTS_ENDPOINT = MAIN_ADMIN_ENDPOINT + "events";
  public static final String ADMIN_STORAGE_STATISTICS = MAIN_ADMIN_ENDPOINT + "statistics/spaces/storage";
  public static final String ADMIN_METRICS_ENDPOINT = MAIN_ADMIN_ENDPOINT + "metrics";

  private static final String ADMIN_CAPABILITY_MESSAGING = "messaging";
  public static final String ADMIN_CAPABILITY_STATISTICS = "statistics";

  public static final String INCLUDE_CHANGES_SINCE = "includeChangesSince";

  public AdminApi(Vertx vertx, Router router, AuthenticationHandler auth) {
    router.route(HttpMethod.POST, ADMIN_MESSAGES_ENDPOINT)
        .handler(BodyHandler.create())
        .handler(auth)
        .handler(this::onMessage);

    router.route(HttpMethod.POST, ADMIN_EVENTS_ENDPOINT)
        .handler(BodyHandler.create())
        .handler(auth)
        .handler(this::onEvent);

    router.route(HttpMethod.GET, ADMIN_STORAGE_STATISTICS)
        .handler(auth)
        .handler(this::onStorageStatistics);

    router.route(HttpMethod.GET, ADMIN_METRICS_ENDPOINT)
        .handler(auth)
        .handler(this::onGetMetrics);
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
      Marker marker = Api.Context.getMarker(context);
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

  /**
   * Posts an event directly to the tasks handlers
   */
  private void onEvent(final RoutingContext context) {
    final Marker marker = Context.getMarker(context);
    final String body = context.body().asString();
    final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);

    try {
      final Event event = XyzSerializable.deserialize(context.body().asString());
      logger.info("Event is " + event.getClass().getSimpleName());
      if (event instanceof LoadFeaturesEvent) {
        new LoadFeaturesQuery((LoadFeaturesEvent) event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
            .execute(this::sendResponse, this::sendErrorResponse);
      } else if (event instanceof GetFeaturesByIdEvent) {
        new IdsQuery((GetFeaturesByIdEvent) event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
            .execute(this::sendResponse, this::sendErrorResponse);
      } else if (event instanceof GetFeaturesByTileEvent) {
        new TileQuery((GetFeaturesByTileEvent) event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
            .execute(this::sendResponse, this::sendErrorResponse);
      } else if (event instanceof GetFeaturesByBBoxEvent) {
        new FeatureTask.BBoxQuery((GetFeaturesByBBoxEvent) event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
            .execute(this::sendResponse, this::sendErrorResponse);
      } else if (event instanceof GetStatisticsEvent) {
        new FeatureTask.GetStatistics((GetStatisticsEvent) event, context, ApiResponseType.STATISTICS_RESPONSE, skipCache)
             .execute(this::sendResponse, this::sendErrorResponse);
      } else if (event instanceof ModifyFeaturesEvent) {
        new FeatureTask.ModifyFeaturesTask((ModifyFeaturesEvent) event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
            .execute(this::sendResponse, this::sendErrorResponse);
      } else if (event instanceof GetFeaturesByGeometryEvent) {
          new GeometryQuery((GetFeaturesByGeometryEvent) event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
              .execute(this::sendResponse, this::sendErrorResponse);
      } else if (event instanceof IterateFeaturesEvent) {
        new IterateQuery((IterateFeaturesEvent) event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
            .execute(this::sendResponse, this::sendErrorResponse);
      }  else if (event instanceof SearchForFeaturesEvent) {
        new SearchQuery((SearchForFeaturesEvent) event, context, ApiResponseType.FEATURE_COLLECTION, skipCache)
            .execute(this::sendResponse, this::sendErrorResponse);
      } else {
        logger.warn("Event cannot be handled: " + body);
        sendErrorResponse(context, new HttpException(HttpResponseStatus.BAD_REQUEST, "Event cannot be handled"));
      }
    } catch (JsonProcessingException e) {
      logger.warn(marker, "Error processing the event payload", e);
      sendErrorResponse(context, new HttpException(BAD_REQUEST, "Error processing the event payload", e));
    } catch (Exception e) {
      logger.error(marker, "General error processing the event", e);
      context.fail(e);
    }
  }

  private static class AdminAuthorization extends Authorization {
    private static void authorizeAdminCapability(RoutingContext context, String capability) throws HttpException {
      JWTPayload jwt = Api.Context.getJWT(context);
      final ActionMatrix tokenRights = jwt.getXyzHubMatrix();
      final XyzHubActionMatrix requestRights = new XyzHubActionMatrix()
          .useAdminCapabilities(XyzHubAttributeMap.forIdValues(capability));

      evaluateRights(Api.Context.getMarker(context), requestRights, tokenRights);
    }
  }
}
