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

package com.here.xyz.hub.task;

import static io.netty.handler.codec.http.HttpResponseStatus.CONFLICT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;

import com.here.xyz.events.ModifySubscriptionEvent;
import com.here.xyz.events.ModifySubscriptionEvent.Operation;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.SpaceConfigClient;
import com.here.xyz.hub.config.TagConfigClient;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.rest.TagApi;
import com.here.xyz.hub.task.FeatureTask.ModifySubscriptionQuery;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Subscription;
import com.here.xyz.models.hub.Tag;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class SubscriptionHandler {

  private static final Logger logger = LogManager.getLogger();

  public static Future<Subscription> getSubscription(RoutingContext context, String spaceId, String subscriptionId) {
    final Marker marker = Api.Context.getMarker(context);

    return Service.subscriptionConfigClient.get(marker, subscriptionId)
        .compose(subscription -> {
          if (!spaceId.equals(subscription.getSource())) {
            logger.warn(marker, "The requested source {} does not match the source {} of the subscription {}", spaceId,
                subscription.getSource(), subscriptionId);
            return Future.failedFuture(new HttpException(NOT_FOUND, "The requested resource does not exist."));
          }

          return Future.succeededFuture(subscription);
        }, t -> {
          logger.warn(marker, "The requested resource does not exist.'", t);
          return Future.failedFuture(new HttpException(NOT_FOUND, "The requested resource does not exist.", t));
        });
  }

  public static Future<List<Subscription>> getSubscriptions(RoutingContext context, String source) {
    final Marker marker = Api.Context.getMarker(context);

    return Service.subscriptionConfigClient.getBySource(marker, source)
        .recover(t -> {
          logger.warn(marker, "Unable to load resource definitions.'", t);
          return Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", t));
        });
  }

  public static Future<Subscription> createSubscription(RoutingContext context, Subscription subscription) {
    final Marker marker = Api.Context.getMarker(context);
    subscription.setStatus(new Subscription.SubscriptionStatus().withState(Subscription.SubscriptionStatus.State.ACTIVE));

    return Service.subscriptionConfigClient.get(marker, subscription.getId())
        .compose(s -> {
          logger.warn(marker, "Resource with the given ID already exists.");
          return Future.failedFuture(new HttpException(CONFLICT, "Resource with the given ID already exists."));
        }, t -> {
          logger.info(marker, "Subscription with id: " + subscription.getId() + " not found.");
          return sendEvent(context, Operation.CREATE, subscription)
              .compose(v -> storeSubscription(marker, subscription));
        });
  }

  public static Future<Subscription> createOrReplaceSubscription(RoutingContext context, Subscription subscription) {
    final Marker marker = Api.Context.getMarker(context);

    // Set status to 'ACTIVE' only when status is not present
    if (subscription.getStatus() == null || subscription.getStatus().getState() == null) {
      subscription.setStatus(new Subscription.SubscriptionStatus().withState(Subscription.SubscriptionStatus.State.ACTIVE));
    }

    return Service.subscriptionConfigClient.get(marker, subscription.getId())
        .map(Operation.UPDATE)
        .otherwise(Operation.CREATE)
        .compose(operation -> sendEvent(context, operation, subscription))
        .compose(v -> storeSubscription(marker, subscription));
  }

  protected static Future<Subscription> storeSubscription(Marker marker, Subscription subscription) {
    logger.info(marker, "storing subscription " + subscription.getId());

    return Service.subscriptionConfigClient.store(marker, subscription)
        .compose(v -> {
          final Future<Void> spaceFuture = SpaceConfigClient.getInstance().get(marker, subscription.getSource())
              .compose(space -> increaseVersionsToKeepIfNecessary(marker, space))
              .recover(t -> {
                logger.info(marker, "spaceFuture for increasing version failed with cause: " + t.getMessage());
                return Future.failedFuture(
                    "Unable to increase versionsToKeep value on space " + subscription.getSource() + " during subscription registration.");
              });
          final Future<Tag> tagFuture = TagConfigClient.getInstance().getTag(marker, subscription.getId(), subscription.getSource())
              .compose(tag -> createTagIfNecessary(marker, tag, subscription.getSource()))
              .recover(t -> {
                logger.info("tagFuture for tag creation failed with cause: " + t.getMessage());
                return Future.failedFuture(
                    "Unable to store tag for space " + subscription.getSource() + " during subscription registration.");
              });

          return CompositeFuture.all(spaceFuture, tagFuture)
              .map(subscription)
              .recover(
                  t -> Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Subscription registration failed.", t.getCause())));
        }, t -> {
          logger.error(marker, "Unable to store resource definition.", t);
          return Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to store the resource definition.", t));
        });
  }

  private static Future<Void> increaseVersionsToKeepIfNecessary(Marker marker, Space space) {
    return space != null && space.getVersionsToKeep() == 1 ?
        SpaceConfigClient.getInstance().store(marker, (com.here.xyz.hub.connectors.models.Space) space.withVersionsToKeep(2)) :
        Future.succeededFuture();
  }

  private static Future<Tag> createTagIfNecessary(Marker marker, Tag tag, String spaceId) {
    return tag == null ?
        TagApi.createTag(marker, spaceId, Service.configuration.SUBSCRIPTION_TAG) :
        Future.succeededFuture(tag);
  }

  public static Future<Subscription> deleteSubscription(RoutingContext context, Subscription subscription) {
    return sendEvent(context, Operation.DELETE, subscription)
        .recover(t -> {
          if (t instanceof HttpException && ((HttpException) t).status.equals(NOT_FOUND)) {
            return Future.succeededFuture();
          }
          return Future.failedFuture(t);
        })
        .compose(v -> removeSubscription(context, subscription));
  }

  protected static Future<Subscription> removeSubscription(RoutingContext context, Subscription subscription) {
    final Marker marker = Api.Context.getMarker(context);

    return Service.subscriptionConfigClient.delete(marker, subscription)
        .compose(s -> Service.subscriptionConfigClient.getBySource(marker, subscription.getSource()))
        .recover(t -> Future.failedFuture(
            new HttpException(INTERNAL_SERVER_ERROR, "Unable retrieve subscription list during subscription de-registration.", t)))
        .compose(subscriptions -> {
          if (CollectionUtils.isNotEmpty(subscriptions)) {
            return Future.succeededFuture(subscription);
          }

          return TagApi.deleteTag(marker, subscription.getSource(), Service.configuration.SUBSCRIPTION_TAG)
              .recover(t -> Future.failedFuture(
                  new HttpException(INTERNAL_SERVER_ERROR, "Unable to delete tag during subscription de-registration.", t)))
              .map(subscription);
        })
        .recover(t -> {
          logger.error(marker, "Unable to delete subscription with id: " + subscription.getId(), t);
          return Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR,
              "Unable to complete subscription deletion for subscription id " + subscription.getId(), t));
        });
  }

  private static Future<Void> sendEvent(RoutingContext context, Operation operation, Subscription subscription) {
    final Promise<Void> promise = Promise.promise();
    final Marker marker = Api.Context.getMarker(context);

    final ModifySubscriptionEvent event = new ModifySubscriptionEvent()
        .withOperation(operation)
        .withSubscription(subscription)
        .withStreamId(marker.getName())
        .withIfNoneMatch(context.request().headers().get("If-None-Match"))
        .withSpace(subscription.getSource());

    logger.info(marker, "ModifySubscriptionEvent to be sent to the connector: " + JsonObject.mapFrom(event));
    final ModifySubscriptionQuery query = new ModifySubscriptionQuery(event, context, ApiResponseType.EMPTY);

    final TaskPipeline.C1<ModifySubscriptionQuery> wrappedSuccessHandler = (t) -> {
      logger.info(marker, "Sending subscription event succeeded");
      promise.tryComplete();
    };

    final TaskPipeline.C2<ModifySubscriptionQuery, Throwable> wrappedExceptionHandler = (t, e) -> {
      logger.info(marker, "Sending subscription event failed");
      promise.tryFail(e);
    };

    query.execute(wrappedSuccessHandler, wrappedExceptionHandler);
    return promise.future();
  }
}
