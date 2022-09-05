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

package com.here.xyz.hub.task;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.models.hub.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.List;

import static io.netty.handler.codec.http.HttpResponseStatus.*;

public class SubscriptionHandler {
    private static final Logger logger = LogManager.getLogger();

    public static void getSubscription(RoutingContext context, String subscriptionId, Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Api.Context.getMarker(context);

        Service.subscriptionConfigClient.get(marker, subscriptionId, ar -> {
            if (ar.failed()) {
                logger.warn(marker, "The requested resource does not exist.'", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(NOT_FOUND, "The requested resource does not exist.", ar.cause())));
            }
            else {
                handler.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    public static void getSubscriptions(RoutingContext context, String source, Handler<AsyncResult<List<Subscription>>> handler) {
        Marker marker = Api.Context.getMarker(context);

        Service.subscriptionConfigClient.getBySource(marker, source, ar -> {
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

        Service.subscriptionConfigClient.getAll(marker, ar -> {
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

        Service.subscriptionConfigClient.get(marker, subscription.getId(), ar -> {
            if (ar.failed()) {
                storeSubscription(context, subscription, handler, marker);
            }
            else {
                logger.info(marker, "Resource with the given ID already exists.");
                handler.handle(Future.failedFuture(new HttpException(BAD_REQUEST, "Resource with the given ID already exists.")));
            }
        });
    }

    protected static void storeSubscription(RoutingContext context, Subscription subscription, Handler<AsyncResult<Subscription>> handler, Marker marker) {

        Service.subscriptionConfigClient.store(marker, subscription, ar -> {
            if (ar.failed()) {
                logger.error(marker, "Unable to store resource definition.'", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to store the resource definition.", ar.cause())));
            } else {
                handler.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

    public static void deleteSubscription(RoutingContext context, String subscriptionId, Handler<AsyncResult<Subscription>> handler) {
        Marker marker = Api.Context.getMarker(context);

        Service.subscriptionConfigClient.delete(marker, subscriptionId, ar -> {
            if (ar.failed()) {
                logger.error(marker, "Unable to delete resource definition.'", ar.cause());
                handler.handle(Future.failedFuture(new HttpException(INTERNAL_SERVER_ERROR, "Unable to delete the resource definition.", ar.cause())));
            } else {
                handler.handle(Future.succeededFuture(ar.result()));
            }
        });
    }

}
