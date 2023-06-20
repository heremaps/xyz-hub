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

package com.here.xyz.hub.task.subscription;

import com.here.xyz.events.admin.ModifySubscriptionEvent;
import com.here.xyz.events.admin.ModifySubscriptionEvent.Operation;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.feature.ModifySubscriptionQuery;
import com.here.naksha.lib.core.models.hub.Subscription;
import com.here.naksha.lib.core.models.hub.SubscriptionStatus;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class SubscriptionHandler {
    private static final Logger logger = LogManager.getLogger();

    public static void getSubscription(RoutingContext context, String spaceId, String subscriptionId, io.vertx.core.Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Context.getMarker(context);

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

    public static void getSubscriptions(RoutingContext context, String source, io.vertx.core.Handler<AsyncResult<List<Subscription>>> handler) {
        Marker marker = Context.getMarker(context);

        Service.subscriptionConfigClient.getBySource(marker, source).onComplete(ar -> {
            if (ar.failed()) {
                logger.warn(marker, "Unable to load resource definitions.'", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", ar.cause())));
            } else {
                handler.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    public static void getAllSubscriptions(RoutingContext context, io.vertx.core.Handler<AsyncResult<List<Subscription>>> handler) {
        Marker marker = Context.getMarker(context);

        Service.subscriptionConfigClient.getAll(marker).onComplete(ar -> {
            if (ar.failed()) {
                logger.warn(marker, "Unable to load resource definitions.'", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to load the resource definitions.", ar.cause())));
            } else {
                handler.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    public static void createSubscription(RoutingContext context, Subscription subscription, io.vertx.core.Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Context.getMarker(context);
        subscription.setStatus(new SubscriptionStatus().withState(SubscriptionStatus.State.ACTIVE));
        Service.subscriptionConfigClient.get(marker, subscription.getId()).onComplete(ar -> {
            if (ar.failed()) {
                // Send ModifySubscriptionEvent to the connector
                sendEvent(context, Operation.CREATE, subscription, false, marker, eventAr -> {
                    if(eventAr.failed()) {
                        handler.handle(Future.failedFuture(eventAr.cause()));
                    } else {
                        storeSubscription(context, subscription, handler, marker);
                    }
                });
            } else {
                logger.warn(marker, "Resource with the given ID already exists.");
                handler.handle(Future.failedFuture(new HttpException(CONFLICT, "Resource with the given ID already exists.")));
            }
        });

    }

    public static void createOrReplaceSubscription(RoutingContext context, Subscription subscription, io.vertx.core.Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Context.getMarker(context);

        // Set status to 'ACTIVE' only when status is not present
        if(subscription.getStatus() == null || subscription.getStatus().getState() == null) {
            subscription.setStatus(new SubscriptionStatus().withState(SubscriptionStatus.State.ACTIVE));
        }

        Service.subscriptionConfigClient.get(marker, subscription.getId()).onComplete(ar -> {
            Operation operation = ar.failed() ? Operation.CREATE : Operation.UPDATE;

            sendEvent(context, operation, subscription, false, marker, eventAr -> {
                if(eventAr.failed()) {
                    handler.handle(Future.failedFuture(eventAr.cause()));
                } else {
                    storeSubscription(context, subscription, handler, marker);
                }
            });
        });
    }

    protected static void storeSubscription(RoutingContext context, Subscription subscription, io.vertx.core.Handler<AsyncResult<Subscription>> handler, Marker marker) {

        Service.subscriptionConfigClient.store(marker, subscription).onComplete(ar -> {
            if (ar.failed()) {
                logger.error(marker, "Unable to store resource definition.", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to store the resource definition.", ar.cause())));
            } else {
                handler.handle(Future.succeededFuture(subscription));
            }
        });
    }

    public static void deleteSubscription(RoutingContext context, Subscription subscription, io.vertx.core.Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Context.getMarker(context);

        getSubscriptions(context, subscription.getSource(), ar -> {
            if(ar.failed()) {
                handler.handle(Future.failedFuture(ar.cause()));
            } else {
                // Check if source space has other ACTIVE subscriptions
                boolean hasActiveSubscriptions = ar.result().stream()
                        .anyMatch(s -> !s.getId().equals(subscription.getId()) &&
                                s.getStatus() != null &&
                                s.getStatus().getState() == SubscriptionStatus.State.ACTIVE);

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

    protected static void removeSubscription(RoutingContext context, Subscription subscription, io.vertx.core.Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Context.getMarker(context);

        Service.subscriptionConfigClient.delete(marker, subscription).onComplete( ar -> {
            if (ar.failed()) {
                logger.error(marker, "Unable to delete resource definition.", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to delete the resource definition.", ar.cause())));
            } else {
                handler.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    private static void sendEvent(RoutingContext context, Operation op, Subscription subscription, boolean hasNoActiveSubscriptions,Marker marker, io.vertx.core.Handler<AsyncResult<Subscription>> handler) {

        ModifySubscriptionEvent event = new ModifySubscriptionEvent()
                .withOperation(op)
                .withSubscription(subscription)
                .ensureStreamId(marker.getName())
                .withIfNoneMatch(context.request().headers().get("If-None-Match"))
                .withSpace(subscription.getSource())
                .withHasNoActiveSubscriptions(hasNoActiveSubscriptions);

        ModifySubscriptionQuery query = new ModifySubscriptionQuery(event, context, ApiResponseType.EMPTY);

        ISuccessHandler<ModifySubscriptionQuery> wrappedSuccessHandler = (t) -> {
            handler.handle(Future.succeededFuture());
        };

        ITaskStep<ModifySubscriptionQuery, Throwable> wrappedExceptionHandler = (t, e) -> {
            handler.handle((Future.failedFuture(e)));
        };

        query.execute(wrappedSuccessHandler, wrappedExceptionHandler);
    }

}
