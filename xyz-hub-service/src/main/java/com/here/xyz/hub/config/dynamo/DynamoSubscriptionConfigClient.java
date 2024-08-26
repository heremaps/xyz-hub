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

package com.here.xyz.hub.config.dynamo;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.PageIterable;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.here.xyz.hub.config.SubscriptionConfigClient;
import com.here.xyz.models.hub.Subscription;
import com.here.xyz.util.service.aws.dynamo.DynamoClient;
import com.here.xyz.util.service.aws.dynamo.IndexDefinition;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class DynamoSubscriptionConfigClient extends SubscriptionConfigClient {

    private static final Logger logger = LogManager.getLogger();

    private final Table subscriptions;
    private final DynamoClient dynamoClient;

    public DynamoSubscriptionConfigClient(String tableArn) {
        dynamoClient = new DynamoClient(tableArn, null);
        logger.debug("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
        subscriptions = dynamoClient.db.getTable(dynamoClient.tableName);
    }

    @Override
    public Future<Void> init() {
        if (dynamoClient.isLocal()) {
            dynamoClient.createTable(subscriptions.getTableName(), "id:S,source:S", "id", List.of(new IndexDefinition("source")), null);
        }

        return Future.succeededFuture();
    }

    @Override
    protected Future<Subscription> getSubscription(Marker marker, String subscriptionId) {
      return dynamoClient.executeQueryAsync(() -> {
        logger.debug(marker, "Getting subscriptionId {} from Dynamo Table {}", subscriptionId, dynamoClient.tableName);
        final Item item = subscriptions.getItem("id", subscriptionId);
        if (item == null)
          return null;
        else
          return Json.decodeValue(item.toJSON(), Subscription.class);
      });
    }

    @Override
    protected Future<List<Subscription>> getSubscriptionsBySource(Marker marker, String source) {
      return dynamoClient.executeQueryAsync(() -> {
        logger.debug(marker, "Getting subscription by source {} from Dynamo Table {}", source, dynamoClient.tableName);
        final PageIterable<Item, QueryOutcome> items = subscriptions.getIndex("source-index").query(new QuerySpec().withHashKey("source", source)).pages();

        List<Subscription> result = new ArrayList<>();
        items.forEach(page -> page.forEach(item -> result.add(Json.decodeValue(item.toJSON(), Subscription.class))));
        return result;
      });
    }

    @Override
    protected Future<List<Subscription>> getAllSubscriptions(Marker marker) {
      return dynamoClient.executeQueryAsync(() -> {
        final List<Subscription> result = new ArrayList<>();
        subscriptions.scan().pages().forEach(p -> p.forEach(i -> {
          final Subscription subscription = Json.decodeValue(i.toJSON(), Subscription.class);
          result.add(subscription);
        }));
        return result;
      });
    }

    @Override
    protected Future<Void> storeSubscription(Marker marker, Subscription subscription) {
      return dynamoClient.executeQueryAsync(() -> {
        logger.debug(marker, "Storing subscription ID {} into Dynamo Table {}", subscription.getId(), dynamoClient.tableName);
        subscriptions.putItem(Item.fromJSON(Json.encode(subscription)));
        return null;
      });
    }

    @Override
    protected Future<Subscription> deleteSubscription(Marker marker, String subscriptionId) {
      return dynamoClient.executeQueryAsync(() -> {
        logger.debug(marker, "Removing subscription with ID {} from Dynamo Table {}", subscriptionId, dynamoClient.tableName);
        DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
            .withPrimaryKey("id", subscriptionId)
            .withReturnValues(ReturnValue.ALL_OLD);
        DeleteItemOutcome response = subscriptions.deleteItem(deleteItemSpec);
        if (response.getItem() != null)
          return Json.decodeValue(response.getItem().toJSON(), Subscription.class);
        else
          throw new RuntimeException("The subscription config was not found for subscription ID: " + subscriptionId);
      });
    }
}
