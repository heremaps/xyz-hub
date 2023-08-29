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

package com.here.xyz.hub.config.memory;

import com.here.xyz.hub.config.ConnectorConfigClient;
import com.here.xyz.hub.connectors.models.Connector;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.Marker;

@SuppressWarnings("unused")
public class InMemConnectorConfigClient extends ConnectorConfigClient {

  private Map<String, Connector> storageMap = new ConcurrentHashMap<>();

  @Override
  protected void getConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    Connector connector = storageMap.get(connectorId);
    handler.handle(Future.succeededFuture(connector));
  }

  @Override
  protected void getConnectorsByOwner(Marker marker, String ownerId, Handler<AsyncResult<List<Connector>>> handler) {
    List<Connector> connectors = storageMap.values().stream().filter(c -> c.owner.equals(ownerId)).collect(Collectors.toList());
    handler.handle(Future.succeededFuture(connectors));
  }

  @Override
  protected void storeConnector(Marker marker, Connector connector, Handler<AsyncResult<Connector>> handler) {
    if (connector.id == null) {
      connector.id = RandomStringUtils.randomAlphanumeric(10);
    }
    storageMap.put(connector.id, connector);
    handler.handle(Future.succeededFuture(connector));
  }

  @Override
  protected void deleteConnector(Marker marker, String connectorId, Handler<AsyncResult<Connector>> handler) {
    Connector connector = storageMap.remove(connectorId);
    handler.handle(Future.succeededFuture(connector));
  }

  @Override
  protected void getAllConnectors(Marker marker, Handler<AsyncResult<List<Connector>>> handler) {
    List<Connector> connectors = new ArrayList<>(storageMap.values());
    handler.handle(Future.succeededFuture(connectors));
  }
}
