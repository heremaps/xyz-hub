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

package com.here.xyz.hub.config;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.dynamo.DynamoSubscriptionConfigClient;
import com.here.xyz.hub.config.jdbc.JDBCSubscriptionConfigClient;
import com.here.xyz.hub.rest.admin.messages.RelayedMessage;
import com.here.xyz.models.hub.Subscription;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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

    public static final ExpiringMap<String, List<Subscription>> cacheBySource = ExpiringMap.builder()
            .expirationPolicy(ExpirationPolicy.CREATED)
            .expiration(10, TimeUnit.MINUTES)
            .build();

    public static SubscriptionConfigClient getInstance() {
        if (Service.configuration.SUBSCRIPTIONS_DYNAMODB_TABLE_ARN != null) {
            return new DynamoSubscriptionConfigClient(Service.configuration.SUBSCRIPTIONS_DYNAMODB_TABLE_ARN);
        } else {
            return JDBCSubscriptionConfigClient.getInstance();
        }
    }

    public Future<Subscription> get(Marker marker, String subscriptionId) {
        final Subscription subscriptionFromCache = cache.get(subscriptionId);

        if (subscriptionFromCache != null) {
            logger.info(marker, "subscriptionId: {} - The subscription was loaded from cache", subscriptionId);
            return Future.succeededFuture(subscriptionFromCache);
        }

        Promise<Subscription> p = Promise.promise();
        getSubscription(marker, subscriptionId).onComplete(ar -> {
            if (ar.succeeded()) {
                final Subscription subscription = ar.result();
                if(subscription == null) {
                    logger.warn(marker, "subscriptionId [{}]: Subscription not found", subscriptionId);
                    p.fail(new RuntimeException("The subscription config was not found for subscription ID: " + subscriptionId));
                } else {
                    cache.put(subscriptionId, subscription);
                    p.complete(subscription);
                }
            }
            else {
                logger.warn(marker, "subscriptionId [{}]: Subscription not found", subscriptionId);
                p.fail(ar.cause());
            }
        });
        return p.future();
    }

    public Future<List<Subscription>> getBySource(Marker marker, String source) {

        final List<Subscription> subscriptionsFromCache = cacheBySource.get(source);

        if (subscriptionsFromCache != null) {
            logger.info(marker, "source: {} - The subscriptions was loaded from cache", source);
            return Future.succeededFuture(subscriptionsFromCache);
        }

        Promise<List<Subscription>> p = Promise.promise();
        getSubscriptionsBySource(marker, source).onComplete( ar -> {
            if (ar.succeeded()) {
                final List<Subscription> subscriptions = ar.result();
                cacheBySource.put(source, subscriptions);
                subscriptions.forEach(s -> {
                    cache.put(s.getId(), s);
                });
                p.complete(subscriptions);
            }
            else {
                logger.warn(marker, "source[{}]: Subscription for source not found", source);
                p.fail(ar.cause());
            }
        });
        return p.future();
    }

    public Future<List<Subscription>> getAll(Marker marker) {
        Promise<List<Subscription>> p = Promise.promise();
        getAllSubscriptions(marker).onComplete( ar -> {
            if (ar.succeeded()) {
                final List<Subscription> subscriptions = ar.result();
                subscriptions.forEach(s -> {
                    cache.put(s.getId(), s);
                });
                p.complete(subscriptions);
            } else {
                logger.error(marker, "Failed to load subscriptions, reason: ", ar.cause());
                p.fail(ar.cause());
            }
        });
        return p.future();
    }

    public Future<Void> store(Marker marker, Subscription subscription) {

        if (subscription.getId() == null) {
            subscription.setId(RandomStringUtils.randomAlphanumeric(10));
        }

        return storeSubscription(marker, subscription)
                .onSuccess(ar -> {
                    invalidateCache(subscription);
                }).onFailure(t -> logger.error(marker, "subscriptionId[{}]: Failed to store subscription configuration, reason: ", subscription.getId(), t));
    }

    public Future<Subscription> delete(Marker marker, Subscription subscription) {
        return deleteSubscription(marker, subscription.getId())
                .onSuccess(ar -> {
                    invalidateCache(subscription);
                }).onFailure(t -> logger.error(marker, "subscriptionId[{}]: Failed to delete subscription configuration, reason: ", subscription.getId(), t));
    }

    protected abstract Future<Subscription> getSubscription(Marker marker, String subscriptionId);

    protected abstract Future<List<Subscription>> getSubscriptionsBySource(Marker marker, String source);

    protected abstract Future<Void> storeSubscription(Marker marker, Subscription subscription);

    protected abstract Future<Subscription> deleteSubscription(Marker marker, String subscriptionId);

    protected abstract Future<List<Subscription>> getAllSubscriptions(Marker marker);

    public void invalidateCache(Subscription subscription) {
        cache.remove(subscription.getId());
        cacheBySource.remove(subscription.getSource());
        new InvalidateSubscriptionCacheMessage().withSubscription(subscription).withGlobalRelay(true).broadcast();
    }

    public static class InvalidateSubscriptionCacheMessage extends RelayedMessage {

        private Subscription subscription;

        public Subscription getSubscription() {
            return subscription;
        }

        public void setSubscription(Subscription subscription) {
            this.subscription = subscription;
        }

        public InvalidateSubscriptionCacheMessage withSubscription(Subscription subscription) {
            this.subscription = subscription;
            return this;
        }

        @Override
        protected void handleAtDestination() {
            cache.remove(subscription.getId());
            cacheBySource.remove(subscription.getSource());
        }
    }
}
