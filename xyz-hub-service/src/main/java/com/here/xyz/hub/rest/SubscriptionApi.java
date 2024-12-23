/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_IMPLEMENTED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.auth.Authorization;
import com.here.xyz.hub.task.SubscriptionHandler;
import com.here.xyz.models.hub.Subscription;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.here.xyz.util.service.HttpException;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class SubscriptionApi extends SpaceBasedApi {
  private static final Logger logger = LogManager.getLogger();
  private static final String JOB_TARGET_PREFIX = "job:";

  public SubscriptionApi(RouterBuilder rb) {
    rb.getRoute("getSubscriptions").setDoValidation(false).addHandler(this::getSubscriptions);
    rb.getRoute("postSubscription").setDoValidation(false).addHandler(this::postSubscription);
    rb.getRoute("getSubscription").setDoValidation(false).addHandler(this::getSubscription);
    rb.getRoute("putSubscription").setDoValidation(false).addHandler(this::putSubscription);
    rb.getRoute("deleteSubscription").setDoValidation(false).addHandler(this::deleteSubscription);
  }

  private void getSubscriptions(final RoutingContext context) {
    try {
      final String spaceId = getSpaceId(context);

      getAndValidateSpace(getMarker(context), spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> SubscriptionHandler.getSubscriptions(context, spaceId))
          .onSuccess(subscription -> sendResponse(context, OK, subscription))
          .onFailure(t -> sendErrorResponse(context, t));
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void postSubscription(final RoutingContext context) {
    try {
      final Marker marker = getMarker(context);
      final String spaceId = getSpaceId(context);
      final Subscription subscription = getSubscriptionInput(context);
      subscription.setSource(spaceId);

      logger.info(marker, "Registering subscription for space " + spaceId + ": " + JsonObject.mapFrom(subscription));
      validateRequestBody(subscription);

      getAndValidateSpace(marker, spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> validateSubscriptionDestination(subscription))
          .compose(v -> SubscriptionHandler.createSubscription(context, subscription))
          .onSuccess(s -> sendResponse(context, CREATED, s))
          .onFailure(t -> sendErrorResponse(context, t));
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void getSubscription(final RoutingContext context) {
    try {
      final String spaceId = getSpaceId(context);
      final String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);

      getAndValidateSpace(getMarker(context), spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> SubscriptionHandler.getSubscription(context, spaceId, subscriptionId))
          .onSuccess(subscription -> sendResponse(context, OK, subscription))
          .onFailure(t -> sendErrorResponse(context, t));
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void putSubscription(final RoutingContext context) {
    try {
      final String spaceId = getSpaceId(context);
      final String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);
      final Subscription subscription = getSubscriptionInput(context);
      subscription.setId(subscriptionId);
      subscription.setSource(spaceId);
      validateRequestBody(subscription);

      getAndValidateSpace(getMarker(context), spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> validateSubscriptionDestination(subscription))
          .compose(v -> SubscriptionHandler.createOrReplaceSubscription(context, subscription))
          .onSuccess(s -> sendResponse(context, OK, s))
          .onFailure(t -> sendErrorResponse(context, t));
    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private void deleteSubscription(final RoutingContext context) {
    try {
      final String spaceId = getSpaceId(context);
      final String subscriptionId = context.pathParam(ApiParam.Path.SUBSCRIPTION_ID);

      Service.spaceConfigClient.get(getMarker(context), spaceId)
          .compose(space -> Authorization.authorizeManageSpacesRights(context, spaceId))
          .compose(v -> SubscriptionHandler.getSubscription(context, spaceId, subscriptionId))
          .compose(subscription -> SubscriptionHandler.deleteSubscription(context, subscription))
          .onSuccess(s -> sendResponse(context, OK, s))
          .onFailure(t -> sendErrorResponse(context, t));

    }
    catch (Exception e) {
      sendErrorResponse(context, e);
    }
  }

  private Subscription getSubscriptionInput(final RoutingContext context) throws HttpException {
    try {
      JsonObject input = context.body().asJsonObject();
      if (input == null)
        throw new HttpException(BAD_REQUEST, "Invalid JSON string");

      return DatabindCodec.mapper().convertValue(input, Subscription.class);
    }
    catch (Exception e) {
      throw new HttpException(BAD_REQUEST, "Invalid JSON string");
    }
  }

  private void validateRequestBody(Subscription subscription) throws HttpException {
    if (subscription.getId() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'id' cannot be empty.");

    if (subscription.getSource() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'source' cannot be empty.");

    if (subscription.getDestination() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property 'destination' cannot be empty.");

    if (subscription.getConfig() == null || subscription.getConfig().getType() == null)
      throw new HttpException(BAD_REQUEST, "Validation failed. The property config 'type' cannot be empty.");
  }

  private Future<Void> validateSubscriptionDestination(Subscription subscription) {
    if (subscription.getDestination().startsWith(JOB_TARGET_PREFIX)) {
      String jobId = subscription.getDestination().substring(JOB_TARGET_PREFIX.length());

      if (Service.configuration.JOB_API_ENDPOINT == null || Service.configuration.JOB_API_ENDPOINT.isEmpty())
        return Future.failedFuture(new HttpException(NOT_IMPLEMENTED, "The subscription with Job destination is not supported."));

      return Service.webClient.getAbs(Service.configuration.JOB_API_ENDPOINT + "/jobs/" + jobId + "/status")
          .send()
          .compose(response -> {
            if (response.statusCode() == 200) {
              JsonObject responseJson = response.bodyAsJsonObject();
              if (responseJson != null && "RUNNING".equals(responseJson.getString("state")))
                return Future.succeededFuture();
              else
                return Future.failedFuture(new IllegalStateException("The destination job " + jobId + " is not in RUNNING state"));

            }
            else if (response.statusCode() == 404)
              return Future.failedFuture(new HttpException(NOT_FOUND, "The destination job " + jobId + " does not exist"));
            else
              return Future.failedFuture(new ValidationException("Failed to validate job " + jobId + " in Job API"));
          });
    }
    return Future.succeededFuture();
  }

}
