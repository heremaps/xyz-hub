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

import static com.here.xyz.hub.task.SpaceConnectorBasedHandler.getAndValidateSpace;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CREATED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.task.SubscriptionHandler;
import com.here.xyz.models.hub.Subscription;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class SubscriptionApi extends Api {

  private static final Logger logger = LogManager.getLogger();

  public SubscriptionApi(RouterBuilder rb) {
    rb.getRoute("getSubscriptions").setDoValidation(false).addHandler(this::getSubscriptions);
    rb.getRoute("postSubscription").setDoValidation(false).addHandler(this::postSubscription);
    rb.getRoute("getSubscription").setDoValidation(false).addHandler(this::getSubscription);
    rb.getRoute("putSubscription").setDoValidation(false).addHandler(this::putSubscription);
    rb.getRoute("deleteSubscription").setDoValidation(false).addHandler(this::deleteSubscription);
  }

  private Subscription getSubscriptionInput(final RoutingContext context) throws HttpException {
    try {
      JsonObject input = context.body().asJsonObject();
      if (input == null) {
        throw new HttpException(BAD_REQUEST, "Invalid JSON string");
      }

      return DatabindCodec.mapper().convertValue(input, Subscription.class);
    } catch (Exception e) {
      throw new HttpException(BAD_REQUEST, "Invalid JSON string");
    }
  }

  private void getSubscription(final RoutingContext context) {
    try {
      final Marker marker = Api.Context.getMarker(context);
      final String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
      final String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);

      getAndValidateSpace(marker, spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> SubscriptionHandler.getSubscription(context, spaceId, subscriptionId))
          .onSuccess(subscription -> sendResponse(context, OK, subscription))
          .onFailure(t -> sendErrorResponse(context, t));
    } catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void getSubscriptions(final RoutingContext context) {
    try {
      final Marker marker = Api.Context.getMarker(context);
      final String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);

      getAndValidateSpace(marker, spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> SubscriptionHandler.getSubscriptions(context, spaceId))
          .onSuccess(subscription -> sendResponse(context, OK, subscription))
          .onFailure(t -> sendErrorResponse(context, t));
    } catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void postSubscription(final RoutingContext context) {
    try {
      final Marker marker = Api.Context.getMarker(context);
      final String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
      final Subscription subscription = getSubscriptionInput(context);
      subscription.setSource(spaceId);

      logger.info(marker, "Registering subscription for space " + spaceId + ": " + JsonObject.mapFrom(subscription));
      validateSubscriptionRequest(subscription);

      getAndValidateSpace(marker, spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> SubscriptionHandler.createSubscription(context, subscription))
          .onSuccess(s -> sendResponse(context, CREATED, s))
          .onFailure(t -> sendErrorResponse(context, t));
    } catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void putSubscription(final RoutingContext context) {
    try {
      final Marker marker = Api.Context.getMarker(context);
      final String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
      final String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);
      final Subscription subscription = getSubscriptionInput(context);
      subscription.setId(subscriptionId);
      subscription.setSource(spaceId);
      validateSubscriptionRequest(subscription);

      getAndValidateSpace(marker, spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> SubscriptionHandler.createOrReplaceSubscription(context, subscription))
          .onSuccess(s -> sendResponse(context, OK, s))
          .onFailure(t -> sendErrorResponse(context, t));
    } catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void deleteSubscription(final RoutingContext context) {
    try {
      final Marker marker = Api.Context.getMarker(context);
      final String spaceId = context.pathParam(ApiParam.Path.SPACE_ID);
      final String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);

      getAndValidateSpace(marker, spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> SubscriptionHandler.getSubscription(context, spaceId, subscriptionId))
          .compose(subscription -> SubscriptionHandler.deleteSubscription(context, subscription))
          .onSuccess(s -> sendResponse(context, OK, s))
          .onFailure(t -> sendErrorResponse(context, t));
    } catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void validateSubscriptionRequest(Subscription subscription) throws HttpException {
    if (subscription.getId() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'id' cannot be empty.");

    if (subscription.getSource() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'source' cannot be empty.");

    if (subscription.getDestination() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'destination' cannot be empty.");

    if (subscription.getConfig() == null || subscription.getConfig().getType() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property config 'type' cannot be empty.");
  }
}
