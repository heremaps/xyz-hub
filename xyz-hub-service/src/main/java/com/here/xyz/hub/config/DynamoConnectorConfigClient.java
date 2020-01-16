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

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
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
      dynamoClient.createTable(connectors.getTableName(), "id:S", "id", null, null);
    }

    onReady.handle(Future.succeededFuture());
  }

  @Override
  protected void getConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    logger.debug(marker, "Getting connectorId {} from Dynamo Table {}", connectorId, dynamoClient.tableName);
    final Item item = connectors.getItem("id", connectorId);

    if (item == null) {
      logger.debug(marker, "connector ID [{}]: This configuration does not exist", connectorId);
      handler.handle(Future.failedFuture("The connector config was not found for connector ID: " + connectorId));
      return;
    }

    final Connector connector = Json.decodeValue(item.toJSON(), Connector.class);
    handler.handle(Future.succeededFuture(connector));
  }

  @Override
  protected void storeConnector(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    logger.debug(marker, "Storing connector ID {} into Dynamo Table {}", connector.id, dynamoClient.tableName);
    connectors.putItem(Item.fromJSON(Json.encode(connector)));
    handler.handle(Future.succeededFuture());
  }

  @Override
  protected void deleteConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    logger.debug(marker, "Removing connector with ID {} from Dynamo Table {}", connectorId, dynamoClient.tableName);
    connectors.deleteItem("id", connectorId);
    handler.handle(Future.succeededFuture());
  }

  @Override
  protected void getAllConnectors(Marker marker, Handler<AsyncResult<List<Connector>>> handler) {
    final List<Connector> result = new ArrayList<>();
    connectors.scan().pages().forEach(p -> p.forEach(i -> {
      final Connector connector = Json.decodeValue(i.toJSON(), Connector.class);
      result.add(connector);
    }));
    handler.handle(Future.succeededFuture(result));
  }
}
