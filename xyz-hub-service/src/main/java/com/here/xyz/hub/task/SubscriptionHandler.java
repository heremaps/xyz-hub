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
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class SubscriptionHandler {
    private static final Logger logger = LogManager.getLogger();

    public static void getSubscription(RoutingContext context, String spaceId, String subscriptionId, Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Api.Context.getMarker(context);

        Service.subscriptionConfigClient.get(marker, subscriptionId).onComplete(ar -> {
            if (ar.failed()) {
                logger.warn(marker, "The requested resource does not exist.'", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(NOT_FOUND, "The requested resource does not exist.", ar.cause())));
            } else {
                Subscription subscription = ar.result();
                if(!spaceId.equals(subscription.getSource())) {
                    logger.warn(marker, "The requested source {} does not match the source {} of the subscription {}", spaceId, subscription.getSource(), subscriptionId);
                    handler.handle(Future.failedFuture(new HttpException(NOT_FOUND, "The requested resource does not exist.", ar.cause())));
                } else {
                    handler.handle(Future.succeededFuture(subscription));
                }
            }
        });
    }

    public static void getSubscriptions(RoutingContext context, String source, Handler<AsyncResult<List<Subscription>>> handler) {
        Marker marker = Api.Context.getMarker(context);

        Service.subscriptionConfigClient.getBySource(marker, source).onComplete(ar -> {
            if (ar.failed()) {
                logger.warn(marker, "Unable to load resource definitions.'", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", ar.cause())));
            } else {
                handler.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    public static void getAllSubscriptions(RoutingContext context, Handler<AsyncResult<List<Subscription>>> handler) {
        Marker marker = Api.Context.getMarker(context);

        Service.subscriptionConfigClient.getAll(marker).onComplete(ar -> {
            if (ar.failed()) {
                logger.warn(marker, "Unable to load resource definitions.'", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", ar.cause())));
            } else {
                handler.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    public static void createSubscription(RoutingContext context, Subscription subscription, Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Api.Context.getMarker(context);
        subscription.setStatus(new Subscription.SubscriptionStatus().withState(Subscription.SubscriptionStatus.State.ACTIVE));
        Service.subscriptionConfigClient.get(marker, subscription.getId()).onComplete(ar -> {
            if (ar.failed()) {
                logger.info(marker, "Getting subscription with id " + subscription.getId() + " failed with reason: " + ar.cause().getMessage());

                // Send ModifySubscriptionEvent to the connector
                sendEvent(context, Operation.CREATE, subscription, false, marker, eventAr -> {
                    if(eventAr.failed()) {
                        handler.handle(Future.failedFuture(eventAr.cause()));
                    } else {
                        storeSubscription(marker, subscription, handler);
                    }
                });
            } else {
                logger.warn(marker, "Resource with the given ID already exists.");
                handler.handle(Future.failedFuture(new HttpException(CONFLICT, "Resource with the given ID already exists.")));
            }
        });

    }

    public static void createOrReplaceSubscription(RoutingContext context, Subscription subscription, Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Api.Context.getMarker(context);

        // Set status to 'ACTIVE' only when status is not present
        if(subscription.getStatus() == null || subscription.getStatus().getState() == null) {
            subscription.setStatus(new Subscription.SubscriptionStatus().withState(Subscription.SubscriptionStatus.State.ACTIVE));
        }

        Service.subscriptionConfigClient.get(marker, subscription.getId()).onComplete(ar -> {
            Operation operation = ar.failed() ? Operation.CREATE : Operation.UPDATE;

            sendEvent(context, operation, subscription, false, marker, eventAr -> {
                if(eventAr.failed()) {
                    handler.handle(Future.failedFuture(eventAr.cause()));
                } else {
                    storeSubscription(marker, subscription, handler);
                }
            });
        });
    }

    protected static void storeSubscription(Marker marker, Subscription subscription, Handler<AsyncResult<Subscription>> handler) {
        logger.info(marker, "storing subscription ");
        Service.subscriptionConfigClient.store(marker, subscription).onComplete(ar -> {
            if (ar.failed()) {
                logger.error(marker, "Unable to store resource definition.", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to store the resource definition.", ar.cause())));
            } else {
                Future<Void> spaceFuture = SpaceConfigClient.getInstance().get(marker, subscription.getSource())
                    .compose(space -> increaseVersionsToKeepIfNecessary(marker, space))
                    .recover(t->Future.failedFuture("Unable to increase versionsToKeep value on space " + subscription.getSource() + " during subscription registration."));
                Future<Tag> tagFuture = TagConfigClient.getInstance().getTag(marker, subscription.getId(), subscription.getSource())
                    .compose(tag -> createTagIfNecessary(marker, tag, subscription.getSource()))
                    .recover(t->Future.failedFuture("Unable to store tag for space " + subscription.getSource() + " during subscription registration."));

                CompositeFuture.all(spaceFuture, tagFuture)
                        .map(cf -> logStoreSubscription(marker, spaceFuture, tagFuture))
                        .onSuccess(none -> handler.handle(Future.succeededFuture(subscription)))
                        .onFailure(t -> handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Subscription registration failed.", t.getCause()))));
                }
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

    private static Future<Void> logStoreSubscription(Marker marker, Future<Void> spaceFuture, Future<Tag> tagFuture) {
      logger.info(marker, "spaceFuture for increasing version " + (spaceFuture.failed() ? "failed with cause " + spaceFuture.cause().getMessage() : "succeeded"));
      logger.info(marker, "tagFuture for tag creation" + (tagFuture.failed() ? "failed with cause " + tagFuture.cause().getMessage() : "succeeded"));
      return Future.succeededFuture();
    }

    public static void deleteSubscription(RoutingContext context, Subscription subscription, Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Api.Context.getMarker(context);

        getSubscriptions(context, subscription.getSource(), ar -> {
            if(ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()));
            } else {
                // Check if source space has other ACTIVE subscriptions
                boolean hasActiveSubscriptions = ar.result().stream()
                        .anyMatch(s -> !s.getId().equals(subscription.getId()) &&
                                s.getStatus() != null &&
                                s.getStatus().getState() == Subscription.SubscriptionStatus.State.ACTIVE);

                sendEvent(context, Operation.DELETE, subscription, !hasActiveSubscriptions, marker, eventAr -> {
                    if(eventAr.failed()) {
                        if(eventAr.cause() instanceof HttpException && ((HttpException) eventAr.cause()).status.equals(NOT_FOUND)) {
                            // Source space not found, delete the subscription directly
                            removeSubscription(context, subscription, handler);
                        } else {
                            handler.handle(Future.failedFuture(eventAr.cause()));
                        }
                    } else {
                        removeSubscription(context, subscription, handler);
                    }
                });

            }
        });
    }

    protected static void removeSubscription(RoutingContext context, Subscription subscription, Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Api.Context.getMarker(context);

        Service.subscriptionConfigClient.delete(marker, subscription).onComplete( ar -> {
            if (ar.failed()) {
                logger.error(marker, "Unable to delete resource definition.", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to delete the resource definition.", ar.cause())));
            } else {
                Service.subscriptionConfigClient.getBySource(marker, subscription.getSource())
                        .onSuccess(list -> {
                            if (list == null || list.isEmpty()) {
                                TagApi.deleteTag(marker, subscription.getSource(), Service.configuration.SUBSCRIPTION_TAG)
                                        .onSuccess(t -> handler.handle(Future.succeededFuture(ar.result())))
                                        .onFailure(t -> handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to delete tag during subscription de-registration.", t.getCause()))));
                            } else {
                                handler.handle(Future.succeededFuture(ar.result()));
                            }
                        })
                        .onFailure(t -> handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable retrieve subscription list during subscription de-registration.", t.getCause()))));
            }
        });
    }

    private static void sendEvent(RoutingContext context, Operation op, Subscription subscription, boolean hasNoActiveSubscriptions,Marker marker, Handler<AsyncResult<Subscription>> handler) {
        ModifySubscriptionEvent event = new ModifySubscriptionEvent()
                .withOperation(op)
                .withSubscription(subscription)
                .withStreamId(marker.getName())
                .withIfNoneMatch(context.request().headers().get("If-None-Match"))
                .withSpace(subscription.getSource());

        logger.info(marker, "ModifySubscriptionEvent to be sent to the connector: " + JsonObject.mapFrom(event));
        ModifySubscriptionQuery query = new ModifySubscriptionQuery(event, context, ApiResponseType.EMPTY);

        TaskPipeline.C1<ModifySubscriptionQuery> wrappedSuccessHandler = (t) -> {
            logger.info(marker, "Sending subscription event succeeded");
            handler.handle(Future.succeededFuture());
        };

        TaskPipeline.C2<ModifySubscriptionQuery, Throwable> wrappedExceptionHandler = (t, e) -> {
            logger.info(marker, "Sending subscription event failed");
            handler.handle((Future.failedFuture(e)));
        };

        query.execute(wrappedSuccessHandler, wrappedExceptionHandler);
    }

}
