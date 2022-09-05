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

package com.here.xyz.hub.config;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.rest.admin.messages.RelayedMessage;
import com.here.xyz.models.hub.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import net.jodah.expiringmap.ExpirationPolicy;
import net.jodah.expiringmap.ExpiringMap;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.List;
import java.util.concurrent.TimeUnit;

public abstract class SubscriptionConfigClient implements Initializable {

    private static final Logger logger = LogManager.getLogger();

    public static final ExpiringMap<String, Subscription> cache = ExpiringMap.builder()
            .expirationPolicy(ExpirationPolicy.CREATED)
            .expiration(1, TimeUnit.MINUTES)
            .build();

    public static SubscriptionConfigClient getInstance() {
        if (Service.configuration.SUBSCRIPTIONS_DYNAMODB_TABLE_ARN != null) {
            return new DynamoSubscriptionConfigClient(Service.configuration.SUBSCRIPTIONS_DYNAMODB_TABLE_ARN);
        } else {
            return JDBCSubscriptionConfigClient.getInstance();
        }
    }

    public void get(Marker marker, String subscriptionId, Handler<AsyncResult<Subscription>> handler) {
        final Subscription subscriptionFromCache = cache.get(subscriptionId);

        if (subscriptionFromCache != null) {
            logger.info(marker, "subscriptionId: {} - The subscription was loaded from cache", subscriptionId);
            handler.handle(Future.succeededFuture(subscriptionFromCache));
            return;
        }

        getSubscription(marker, subscriptionId, ar -> {
            if (ar.succeeded()) {
                final Subscription subscription = ar.result();
                cache.put(subscriptionId, subscription);
                handler.handle(Future.succeededFuture(subscription));
            }
            else {
                logger.warn(marker, "subscriptionId[{}]: Subscription not found", subscriptionId);
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void getBySource(Marker marker, String source, Handler<AsyncResult<List<Subscription>>> handler) {
        getSubscriptionsBySource(marker, source, ar -> {
            if (ar.succeeded()) {
                final List<Subscription> subscriptions = ar.result();
                subscriptions.forEach(s -> {
                    cache.put(s.getId(), s);
                });
                handler.handle(Future.succeededFuture(subscriptions));
            }
            else {
                logger.warn(marker, "source[{}]: Subscription for source not found", source);
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void getAll(Marker marker, Handler<AsyncResult<List<Subscription>>> handler) {
        getAllSubscriptions(marker, ar -> {
            if (ar.succeeded()) {
                final List<Subscription> subscriptions = ar.result();
                subscriptions.forEach(s -> {
                    cache.put(s.getId(), s);
                });
                handler.handle(Future.succeededFuture(subscriptions));
            } else {
                logger.error(marker, "Failed to load subscriptions, reason: ", ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void store(Marker marker, Subscription subscription, Handler<AsyncResult<Subscription>> handler) {
        store(marker, subscription, handler, true);
    }

    private void store(Marker marker, Subscription subscription, Handler<AsyncResult<Subscription>> handler, boolean withInvalidation) {
        if (subscription.getId() == null) {
            subscription.setId(RandomStringUtils.randomAlphanumeric(10));
        }

        storeSubscription(marker, subscription, ar -> {
            if (ar.succeeded()) {
                final Subscription subscriptionResult = ar.result();
                if (withInvalidation) {
                    invalidateCache(subscription.getId());
                }
                handler.handle(Future.succeededFuture(subscriptionResult));
            } else {
                logger.error(marker, "subscriptionId[{}]: Failed to store subscription configuration, reason: ", subscription.getId(), ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    public void delete(Marker marker, String subscriptionId, Handler<AsyncResult<Subscription>> handler) {
        deleteSubscription(marker, subscriptionId, ar -> {
            if (ar.succeeded()) {
                final Subscription subscriptionResult = ar.result();
                invalidateCache(subscriptionId);
                handler.handle(Future.succeededFuture(subscriptionResult));
            } else {
                logger.error(marker, "subscriptionId[{}]: Failed to delete subscription configuration, reason: ", subscriptionId, ar.cause());
                handler.handle(Future.failedFuture(ar.cause()));
            }
        });
    }

    protected abstract void getSubscription(Marker marker, String subscriptionId, Handler<AsyncResult<Subscription>> handler);

    protected abstract void getSubscriptionsBySource(Marker marker, String source, Handler<AsyncResult<List<Subscription>>> handler);

    protected abstract void storeSubscription(Marker marker, Subscription subscription, Handler<AsyncResult<Subscription>> handler);

    protected abstract void deleteSubscription(Marker marker, String subscriptionId, Handler<AsyncResult<Subscription>> handler);

    protected abstract void getAllSubscriptions(Marker marker, Handler<AsyncResult<List<Subscription>>> handler);

    public void invalidateCache(String subscriptionId) {
        cache.remove(subscriptionId);
        new SubscriptionConfigClient.InvalidateSpaceCacheMessage().withId(subscriptionId).withGlobalRelay(true).broadcast();
    }

    public static class InvalidateSpaceCacheMessage extends RelayedMessage {

        private String id;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public SubscriptionConfigClient.InvalidateSpaceCacheMessage withId(String id) {
            this.id = id;
            return this;
        }

        @Override
        protected void handleAtDestination() {
            cache.remove(id);
        }
    }
}
