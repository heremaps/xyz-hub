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

package com.here.xyz.hub.config.tmp;

import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.ConnectorConfigClient;
import com.here.xyz.hub.config.DynamoConnectorConfigClient;
import com.here.xyz.hub.config.Initializable;
import com.here.xyz.hub.config.JDBCConnectorConfigClient;
import com.here.xyz.hub.connectors.models.Connector;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class MigratingConnectorConfigClient extends ConnectorConfigClient {

  private static final Logger logger = LogManager.getLogger();

  private static MigratingConnectorConfigClient instance;
  private static boolean initialized = false;
  private ConnectorConfigClient newConnectorClient;
  private ConnectorConfigClient oldConnectorClient;

  private MigratingConnectorConfigClient() {
    this.newConnectorClient = new DynamoConnectorConfigClient(Service.configuration.CONNECTORS_DYNAMODB_TABLE_ARN);
    this.oldConnectorClient = JDBCConnectorConfigClient.getInstance();
  }

  public static MigratingConnectorConfigClient getInstance() {
    if (instance == null) {
      instance = new MigratingConnectorConfigClient();
    }
    return instance;
  }

  @Override
  protected void getConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    oldConnectorClient.get(marker, connectorId, ar -> {
      if (ar.failed() || ar.result() == null) {
        newConnectorClient.get(marker, connectorId, handler);
      } else {
        handler.handle(ar);
        //The connector was still in the old config-storage so we need to migrate it
        moveConnector(marker, ar.result(), migrationResult -> {
        });
      }
    });
  }

  @Override
  protected void storeConnector(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    newConnectorClient.store(marker, connector, handler);
  }

  @Override
  protected void deleteConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    oldConnectorClient.delete(marker, connectorId, ar -> {
      if (ar.failed()) {
        handler.handle(ar);
      } else {
        newConnectorClient.delete(marker, connectorId, handler);
      }
    });
  }

  @Override
  public void getAllConnectors(Marker marker, Handler<AsyncResult<List<Connector>>> handler) {
    oldConnectorClient.getAll(marker, oldResult -> {
      if (oldResult.failed()) {
        handler.handle(oldResult);
      } else {
        newConnectorClient.getAll(marker, newResult -> {
          if (newResult.failed()) {
            handler.handle(newResult);
          } else {
            List<Connector> connectors = new ArrayList<>(oldResult.result());
            connectors.addAll(newResult.result());
            handler.handle(Future.succeededFuture(connectors));
          }
        });
      }
    });
  }

  @Override
  public synchronized void init(Handler<AsyncResult<Void>> onReady) {
    if (initialized) {
      onReady.handle(Future.succeededFuture());
      return;
    }

    initialized = true;

    CompositeFuture.all(
        initFuture(oldConnectorClient),
        initFuture(newConnectorClient)
    ).setHandler(h -> onReady.handle(Future.succeededFuture()));
  }

  private <T> Future<T> initFuture(Initializable objectToInit) {
    return Future.future(h -> {
      objectToInit.init(h2 -> {
        if (h2.failed()) {
          h.handle(Future.failedFuture(h2.cause()));
        } else {
          h.handle(Future.succeededFuture());
        }
      });
    });
  }

  private void moveConnector(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    newConnectorClient.store(marker, connector, storeResult -> {
      if (storeResult.failed()) {
        logger
            .error(marker, "Error when trying to store connector while migrating it. Connector ID: " + connector.id, storeResult.cause());
        return;
      }
      oldConnectorClient.delete(marker, connector.id, deletionResult -> {
        if (deletionResult.failed()) {
          logger.error(marker, "Error when trying to delete old connector while migrating it. Connector ID: " + connector.id,
              deletionResult.cause());
          return;
        }
        handler.handle(Future.succeededFuture(connector));
      });
    });
  }
}
