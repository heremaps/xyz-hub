/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * A client for reading and editing xyz space and connector definitions.
 */
public class JDBCConnectorConfigClient extends ConnectorConfigClient {

  private static final Logger logger = LogManager.getLogger();

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
    final SQLQuery query = new SQLQuery("SELECT config FROM " + CONNECTOR_TABLE + " WHERE id = ?", connectorId);
    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        final Optional<String> config = out.result().getRows().stream().map(r -> r.getString("config")).findFirst();
        if (config.isPresent()) {
          final Connector connector = Json.decodeValue(config.get(), Connector.class);
          logger.debug(marker, "storageId[{}]: Loaded connector from the database.", connectorId);
          handler.handle(Future.succeededFuture(connector));
        } else {
          logger.debug(marker, "storageId[{}]: This configuration does not exist", connectorId);
          handler.handle(Future.failedFuture("The connector config not found for storageId: " + connectorId));
        }
      } else {
        logger.debug(marker, "storageId[{}]: Failed to load configuration, reason: ", connectorId, out.cause());
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }

  @Override
  protected void getConnectorsByOwner(Marker marker, String ownerId, Handler<AsyncResult<List<Connector>>> handler) {
    final SQLQuery query = new SQLQuery("SELECT config FROM " + CONNECTOR_TABLE + " WHERE owner = ?", ownerId);
    client.queryWithParams(query.text(), new JsonArray(query.parameters()), out -> {
      if (out.succeeded()) {
        final Stream<String> config = out.result().getRows().stream().map(r -> r.getString("config"));
        List<Connector> result = new ArrayList<>();
        config.forEach(c -> {
          if (c != null) {
            final Connector connector = Json.decodeValue(c, Connector.class);
            result.add(connector);
            logger.debug(marker, "ownerId[{}]: Loaded connectors from the database.", ownerId);
          }
        });
        handler.handle(Future.succeededFuture(result));

      } else {
        logger.debug(marker, "ownerId[{}]: Failed to load configurations, reason: ", ownerId, out.cause());
        handler.handle(Future.failedFuture(out.cause()));
      }
    });
  }

  @Override
  protected void storeConnector(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    final SQLQuery query = new SQLQuery("INSERT INTO " + CONNECTOR_TABLE + " (id, owner, config) VALUES (?, ?, cast(? as JSONB)) " +
        "ON CONFLICT (id) DO " +
        "UPDATE SET id = ?, owner = ?, config = cast(? as JSONB)",
        connector.id, connector.owner, Json.encode(connector),
        connector.id, connector.owner, Json.encode(connector));
    updateWithParams(connector, query, handler);
  }

  @Override
  protected void deleteConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    final SQLQuery query = new SQLQuery("DELETE FROM " + CONNECTOR_TABLE + " WHERE id = ?", connectorId);
    get(marker, connectorId, ar -> {
      if (ar.succeeded()) {
        updateWithParams(ar.result(), query, handler);
      } else {
        logger.error(ar.cause());
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
    client.query("SELECT config FROM " + CONNECTOR_TABLE, out -> {
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
