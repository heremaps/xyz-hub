/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.PageIterable;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.here.xyz.models.hub.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

import java.util.ArrayList;
import java.util.List;

public class DynamoSubscriptionConfigClient extends SubscriptionConfigClient {

    private static final Logger logger = LogManager.getLogger();

    private final Table subscriptions;
    private final DynamoClient dynamoClient;

    public DynamoSubscriptionConfigClient(String tableArn) {
        dynamoClient = new DynamoClient(tableArn);
        logger.debug("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
        subscriptions = dynamoClient.db.getTable(dynamoClient.tableName);
    }

    @Override
    public void init(Handler<AsyncResult<Void>> onReady) {
        if (dynamoClient.isLocal()) {
            dynamoClient.createTable(subscriptions.getTableName(), "id:S,source:S", "id", "source", null);
        }

        onReady.handle(Future.succeededFuture());
    }

    @Override
    protected Future<Subscription> getSubscription(Marker marker, String subscriptionId) {
        return DynamoClient.dynamoWorkers.executeBlocking(
                future -> {
                    try {
                        logger.debug(marker, "Getting subscriptionId {} from Dynamo Table {}", subscriptionId, dynamoClient.tableName);
                        final Item item = subscriptions.getItem("id", subscriptionId);
                        if(item == null) {
                            future.complete();
                        } else {
                            final Subscription subscription = Json.decodeValue(item.toJSON(), Subscription.class);
                            future.complete(subscription);
                        }
                    }
                    catch (Exception e) {
                        future.fail(e);
                    }
                }
        );
    }

    @Override
    protected Future<List<Subscription>> getSubscriptionsBySource(Marker marker, String source) {
        return DynamoClient.dynamoWorkers.executeBlocking(
                future -> {
                    try {
                        logger.debug(marker, "Getting subscription by source {} from Dynamo Table {}", source, dynamoClient.tableName);
                        final PageIterable<Item, QueryOutcome> items = subscriptions.getIndex("source-index").query(new QuerySpec().withHashKey("source", source)).pages();

                        List<Subscription> result = new ArrayList<>();
                        items.forEach(page -> page.forEach(item -> result.add(Json.decodeValue(item.toJSON(), Subscription.class))));
                        future.complete(result);
                    }
                    catch (Exception e) {
                        future.fail(e);
                    }
                }
        );
    }

    @Override
    protected Future<List<Subscription>> getAllSubscriptions(Marker marker) {
        return DynamoClient.dynamoWorkers.executeBlocking( future -> {
                try {
                    final List<Subscription> result = new ArrayList<>();
                    subscriptions.scan().pages().forEach(p -> p.forEach(i -> {
                        final Subscription subscription = Json.decodeValue(i.toJSON(), Subscription.class);
                        result.add(subscription);
                    }));
                    future.complete(result);
                }
                catch (Exception e) {
                    future.fail(e);
                }
            }
        );
    }

    @Override
    protected Future<Void> storeSubscription(Marker marker, Subscription subscription) {
        logger.debug(marker, "Storing subscription ID {} into Dynamo Table {}", subscription.getId(), dynamoClient.tableName);
        return DynamoClient.dynamoWorkers.executeBlocking(
                future -> {
                    try {
                        subscriptions.putItem(Item.fromJSON(Json.encode(subscription)));
                        future.complete();
                    }
                    catch (Exception e) {
                        future.fail(e);
                    }
                }
        );
    }

    @Override
    protected Future<Subscription> deleteSubscription(Marker marker, String subscriptionId) {
        logger.debug(marker, "Removing subscription with ID {} from Dynamo Table {}", subscriptionId, dynamoClient.tableName);
        return DynamoClient.dynamoWorkers.executeBlocking(future -> {
                    try {
                        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                                .withPrimaryKey("id", subscriptionId)
                                .withReturnValues(ReturnValue.ALL_OLD);
                        DeleteItemOutcome response = subscriptions.deleteItem(deleteItemSpec);
                        if (response.getItem() != null)
                            future.complete(Json.decodeValue(response.getItem().toJSON(), Subscription.class));
                        else
                            future.fail(new RuntimeException("The subscription config was not found for subscription ID: " + subscriptionId));
                    }
                    catch (Exception e) {
                        future.fail(e);
                    }
                }
        );
    }
}
