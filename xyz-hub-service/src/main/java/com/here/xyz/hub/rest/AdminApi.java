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
import static com.here.xyz.hub.rest.ApiParam.Query.SKIP_CACHE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.hub.auth.ActionMatrix;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.auth.XyzHubAttributeMap;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.rest.admin.MessageBroker;
import com.here.xyz.hub.task.FeatureTask;
import com.here.xyz.hub.task.FeatureTask.IdsQuery;
import com.here.xyz.hub.task.FeatureTask.LoadFeaturesQuery;
import com.here.xyz.hub.task.FeatureTask.TileQuery;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class AdminApi extends Api {
  private static final Logger logger = LogManager.getLogger();

  public static final String MAIN_ADMIN_ENDPOINT = "/hub/admin/";
  public static final String ADMIN_MESSAGES_ENDPOINT = MAIN_ADMIN_ENDPOINT + "messages";
  public static final String ADMIN_EVENTS_ENDPOINT = MAIN_ADMIN_ENDPOINT + "events";
  private static final MessageBroker messageBroker = MessageBroker.getInstance();

  private static final String ADMIN_CAPABILITY_MESSAGING = "messaging";

  public AdminApi(Vertx vertx, Router router, AuthHandler auth) {
    router.route(HttpMethod.POST, ADMIN_MESSAGES_ENDPOINT)
        .handler(auth)
        .handler(this::onMessage);

    router.route(HttpMethod.POST, ADMIN_EVENTS_ENDPOINT)
        .handler(auth)
        .handler(this::onEvent);
  }

  private void onMessage(final RoutingContext context) {
    try {
      AdminAuthorization.authorizeAdminMessaging(context);
      messageBroker.receiveRawMessage(context.getBody().getBytes());
      context.response().setStatusCode(NO_CONTENT.code())
          .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
          .end();
    }
    catch (HttpException e) {
      sendErrorResponse(context, e);
    }
  }

  /**
   * Posts a event directly to the tasks handlers
   */
  private void onEvent(final RoutingContext context) {
    final Marker marker = Context.getMarker(context);
    final String body = context.getBodyAsString();
    final boolean skipCache = Query.getBoolean(context, SKIP_CACHE, false);

    try {
      final Event event = XyzSerializable.deserialize(context.getBodyAsString());
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
      } else {
        logger.warn("Event cannot be handled: " + body);
        sendErrorResponse(context, new HttpException(HttpResponseStatus.BAD_REQUEST, "Event cannot be handled"));
      }
    } catch (JsonProcessingException e) {
      logger.error(marker, "Error processing the event payload", e);
      sendErrorResponse(context, new HttpException(BAD_REQUEST, "Error processing the event payload", e));
    } catch (Exception e) {
      logger.error(marker, "General error processing the event", e);
      context.fail(e);
    }
  }

  private static class AdminAuthorization extends Authorization {
    public static void authorizeAdminMessaging(RoutingContext context) throws HttpException {
      JWTPayload jwt = Api.Context.getJWT(context);
      final ActionMatrix tokenRights = jwt.getXyzHubMatrix();
      final XyzHubActionMatrix requestRights = new XyzHubActionMatrix()
          .useAdminCapabilities(XyzHubAttributeMap.forIdValues(ADMIN_CAPABILITY_MESSAGING));

      evaluateRights(Api.Context.getMarker(context), requestRights, tokenRights);
    }
  }
}
