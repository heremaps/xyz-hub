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

import static com.here.xyz.hub.config.JDBCConfig.CONNECTOR_TABLE;

import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.psql.SQLQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Marker;

/**
 * A client for reading and editing xyz space and connector definitions.
 */
public class JDBCConnectorConfigClient extends ConnectorConfigClient {

  private static JDBCConnectorConfigClient instance;
  private final SQLClient client;

  private JDBCConnectorConfigClient() {
    this.client = JDBCConfig.getClient();
  }

  public static JDBCConnectorConfigClient getInstance() {
    if (instance == null) {
      instance = new JDBCConnectorConfigClient();
    }
    return instance;
  }

  @Override
  public void init(Handler<AsyncResult<Void>> onReady) {
    JDBCConfig.init(onReady);
  }

  @Override
  protected void getConnector(final Marker marker, final String connectorId, final Handler<AsyncResult<Connector>> handler) {
    final SQLQuery query = new SQLQuery(String.format("SELECT config FROM %s WHERE id = ?", CONNECTOR_TABLE), connectorId);
    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        final Optional<String> config = out.result().getRows().stream().map(r -> r.getString("config")).findFirst();
        if (config.isPresent()) {
          final Connector connector = Json.decodeValue(config.get(), Connector.class);
          logger().debug(marker, "storageId[{}]: Loaded connector from the database.", connectorId);
          handler.handle(Future.succeededFuture(connector));
        } else {
          logger().debug(marker, "storageId[{}]: This configuration does not exist", connectorId);
          handler.handle(Future.failedFuture("The connector config not found for storageId: " + connectorId));
        }
      } else {
        logger().debug(marker, "storageId[{}]: Failed to load configuration, reason: ", connectorId, out.cause());
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }

  @Override
  protected void storeConnector(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    final SQLQuery query = new SQLQuery(String.format("INSERT INTO %s(id, config) VALUES (?, cast(? as JSONB))", CONNECTOR_TABLE),
        connector.id, Json.encode(connector));
    updateWithParams(connector, query, handler);
  }

  @Override
  protected void deleteConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    final SQLQuery query = new SQLQuery(String.format("DELETE FROM %s WHERE id = ?", CONNECTOR_TABLE), connectorId);
    get(marker, connectorId, ar -> {
      if (ar.succeeded()) {
        updateWithParams(ar.result(), query, handler);
      } else {
        handler.handle(Future.failedFuture(ar.cause()));
      }
    });
  }

  private void updateWithParams(Connector modifiedObject, SQLQuery query, Handler<AsyncResult<Connector>> handler) {
    client.updateWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        handler.handle(Future.succeededFuture(modifiedObject));
      } else {
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }

  @Override
  protected void getAllConnectors(Marker marker, Handler<AsyncResult<List<Connector>>> handler) {
    client.query(String.format("SELECT config FROM %s", CONNECTOR_TABLE), out -> {
      if (out.succeeded()) {
        List<Connector> configs = out.result().getRows().stream()
            .map(r -> r.getString("config"))
            .map(json -> Json.decodeValue(json, Connector.class))
            .collect(Collectors.toList());
        handler.handle(Future.succeededFuture(configs));

      } else {
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }
}
