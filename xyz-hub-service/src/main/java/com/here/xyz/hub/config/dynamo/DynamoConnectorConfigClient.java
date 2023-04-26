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

package com.here.xyz.hub.config.dynamo;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.PageIterable;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.here.xyz.hub.config.ConnectorConfigClient;
import com.here.xyz.hub.connectors.models.Connector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class DynamoConnectorConfigClient extends ConnectorConfigClient {

  private static final Logger logger = LogManager.getLogger();

  private final Table connectors;
  private final DynamoClient dynamoClient;

  public DynamoConnectorConfigClient(String tableArn) {
    dynamoClient = new DynamoClient(tableArn);
    logger.debug("Instantiating a reference to Dynamo Table {}", dynamoClient.tableName);
    connectors = dynamoClient.db.getTable(dynamoClient.tableName);
  }

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    if (dynamoClient.isLocal()) {
      dynamoClient.createTable(connectors.getTableName(), "id:S,owner:S", "id", "owner", null);
    }

    onReady.handle(Future.succeededFuture());
  }

  @Override
  protected void getConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    DynamoClient.dynamoWorkers.executeBlocking(
        future -> {
          try {
            logger.debug(marker, "Getting connectorId {} from Dynamo Table {}", connectorId, dynamoClient.tableName);
            final Item item = connectors.getItem("id", connectorId);
            future.complete(item);
          }
          catch (Exception e) {
            future.fail(e);
          }
        },
        ar -> {
          if (ar.failed()) {
            logger.error(marker, "Error getting connector with ID {}", connectorId, ar.cause());
            handler.handle(Future.failedFuture(new RuntimeException("Error getting connector with ID " + connectorId, ar.cause())));
          }
          else {
            Item item = (Item) ar.result();
            if (item == null) {
              logger.debug(marker, "connector ID [{}]: This configuration does not exist", connectorId);
              handler.handle(Future.failedFuture(new RuntimeException("The connector config was not found for connector ID: " + connectorId)));
              return;
            }

            final Connector connector = Json.decodeValue(item.toJSON(), Connector.class);
            handler.handle(Future.succeededFuture(connector));
          }
        }
    );
  }

  @Override
  protected void getConnectorsByOwner(Marker marker, String ownerId, Handler<AsyncResult<List<Connector>>> handler) {
    DynamoClient.dynamoWorkers.executeBlocking(
        future -> {
          try {
            logger.debug(marker, "Getting connectors by owner {} from Dynamo Table {}", ownerId, dynamoClient.tableName);
            final PageIterable<Item, QueryOutcome> items = connectors.getIndex("owner-index").query(new QuerySpec().withHashKey("owner", ownerId)).pages();
            future.complete(items);
          }
          catch (Exception e) {
            future.fail(e);
          }
        },
        ar -> {
          if (ar.failed()) {
            logger.error(marker, "Error getting connectors for owner {}", ownerId, ar.cause());
            handler.handle(Future.failedFuture("Error getting connectors for owner " + ownerId));
          }
          else {
            PageIterable<Item, QueryOutcome> items = (PageIterable<Item, QueryOutcome>) ar.result();
            List<Connector> result = new ArrayList<>();
            items.forEach(page -> page.forEach(item -> result.add(Json.decodeValue(item.toJSON(), Connector.class))));
            handler.handle(Future.succeededFuture(result));
          }
        }
    );
  }

  @Override
  protected void storeConnector(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    logger.debug(marker, "Storing connector ID {} into Dynamo Table {}", connector.id, dynamoClient.tableName);
    DynamoClient.dynamoWorkers.executeBlocking(
        future -> {
          try {
            connectors.putItem(Item.fromJSON(Json.encode(connector)));
            future.complete();
          }
          catch (Exception e) {
            future.fail(e);
          }
        },
        ar -> {
          if (ar.failed()) {
            logger.error(marker, "Error while storing connector.", ar.cause());
            handler.handle(Future.failedFuture("Error while storing connector."));
          }
          else {
            handler.handle(Future.succeededFuture(connector));
          }
        }
    );
  }

  @Override
  protected void deleteConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    logger.debug(marker, "Removing connector with ID {} from Dynamo Table {}", connectorId, dynamoClient.tableName);
    DynamoClient.dynamoWorkers.executeBlocking(
        future -> {
          try {
            DeleteItemSpec deleteItemSpec = new DeleteItemSpec()
                .withPrimaryKey("id", connectorId)
                .withReturnValues(ReturnValue.ALL_OLD);
            DeleteItemOutcome response = connectors.deleteItem(deleteItemSpec);
            if (response.getItem() != null)
              future.complete(Json.decodeValue(response.getItem().toJSON(), Connector.class));
            else
              future.fail(new RuntimeException("The connector config was not found for connector ID: " + connectorId));
          }
          catch (Exception e) {
            future.fail(e);
          }
        },
        ar -> {
          if (ar.failed()) {
            logger.error(marker, "Error while deleting connector.", ar.cause());
            handler.handle(Future.failedFuture("Error while deleting connector."));
          }
          else {
            handler.handle(Future.succeededFuture((Connector) ar.result()));
          }
        }
    );
  }

  @Override
  protected void getAllConnectors(Marker marker, Handler<AsyncResult<List<Connector>>> handler) {
    DynamoClient.dynamoWorkers.executeBlocking(
        future -> {
          try {
            final List<Connector> result = new ArrayList<>();
            connectors.scan().pages().forEach(p -> p.forEach(i -> {
              final Connector connector = Json.decodeValue(i.toJSON(), Connector.class);
              result.add(connector);
            }));
            future.complete(result);
          }
          catch (Exception e) {
            future.fail(e);
          }
        },
        ar -> {
          if (ar.failed()) {
            logger.error(marker, "Error retrieving all connectors.", ar.cause());
            handler.handle(Future.failedFuture(new RuntimeException("Error retrieving all connectors.", ar.cause())));
          }
          else {
            handler.handle(Future.succeededFuture((List<Connector>) ar.result()));
          }
        }
    );
  }
}
