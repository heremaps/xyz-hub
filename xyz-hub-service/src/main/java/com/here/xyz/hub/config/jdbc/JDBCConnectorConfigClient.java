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

package com.here.xyz.hub.config.jdbc;

import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.SCHEMA;
import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.configListParser;
import static com.here.xyz.hub.config.jdbc.JDBCConfigClient.configParser;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.ConnectorConfigClient;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.Json;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

/**
 * A client for reading and editing xyz space and connector definitions.
 */
public class JDBCConnectorConfigClient extends ConnectorConfigClient {

  private static final Logger logger = LogManager.getLogger();

  private static JDBCConnectorConfigClient instance;
  private static final String CONNECTOR_TABLE = "xyz_storage";
  private final JDBCConfigClient client = new JDBCConfigClient(SCHEMA, CONNECTOR_TABLE, Service.configuration);

  public static JDBCConnectorConfigClient getInstance() {
    if (instance == null)
      instance = new JDBCConnectorConfigClient();
    return instance;
  }

  @Override
  protected Future<Connector> getConnector(final Marker marker, final String connectorId) {
    final SQLQuery query = client.getQuery("SELECT config FROM ${schema}.${table} WHERE id = #{connectorId}")
        .withNamedParameter("connectorId", connectorId);

    return client.run(query, configParser(Connector.class))
        .compose(connector -> {
          if (connector == null) {
            logger.debug(marker, "The connector {} does not exist", connectorId);
            return Future.failedFuture("The connector config not found for storageId: " + connectorId);
          }
          return Future.succeededFuture(connector);
        })
        .onSuccess(connector -> logger.debug(marker, "Loaded connector {} from the database.", connectorId))
        .onFailure(t -> logger.debug(marker, "Failed to load connector {}", connectorId, t));
  }

  @Override
  protected Future<List<Connector>> getConnectorsByOwner(Marker marker, String ownerId) {
    final SQLQuery query = client.getQuery("SELECT config FROM ${schema}.${table} WHERE owner = #{ownerId}")
        .withNamedParameter("ownerId", ownerId);

    return client.run(query, configListParser(Connector.class))
        .onSuccess(connectors -> {
          logger.debug(marker, "Loaded connectors for owner {} from the database.", ownerId);
        })
        .onFailure(t -> logger.debug(marker, "ownerId[{}]: Failed to load configurations, reason: ", ownerId, t));
  }

  @Override
  protected void storeConnector(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    final SQLQuery query = client.getQuery("INSERT INTO ${schema}.${table} (id, owner, config) VALUES (#{connectorId}, #{owner}, cast(#{connectorJson} as JSONB)) " +
        "ON CONFLICT (id) DO " +
        "UPDATE SET id = #{connectorId}, owner = #{owner}, config = cast(#{connectorJson} as JSONB)")
        .withNamedParameter("connectorId", connector.id)
        .withNamedParameter("owner", connector.owner)
        .withNamedParameter("connectorJson", Json.encode(connector)); //TODO: Use XyzSerializable with static view
    client.write(query).map(connector).andThen(handler);
  }

  @Override
  protected void deleteConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    final SQLQuery query = client.getQuery("DELETE FROM ${schema}.${table} WHERE id = #{connectorId}")
        .withNamedParameter("connectorId", connectorId);

    getConnector(marker, connectorId)
        .compose(connector -> client.write(query).map(connector))
        .onFailure(t -> logger.error(t))
        .andThen(handler);
  }

  @Override
  protected void getAllConnectors(Marker marker, Handler<AsyncResult<List<Connector>>> handler) {
    client.run(client.getQuery("SELECT config FROM ${schema}.${table}"), configListParser(Connector.class)).andThen(handler);
  }

  @Override
  public Future<Void> init() {
    return client.init()
        .compose(v -> initTable());
  }

  private Future<Void> initTable() {
    return client.write(client.getQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} "
            + "(id TEXT primary key, owner TEXT, config JSONB)"))
        .onFailure(e -> logger.error("Can not create table {}!", CONNECTOR_TABLE, e))
        .mapEmpty();
  }
}
