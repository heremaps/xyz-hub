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

package com.here.xyz.httpconnector.task;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.web.LegacyHubWebClient;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.ECPSTool;
import com.here.xyz.util.db.JdbcClient;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class JdbcBasedHandler {
  private static final Logger logger = LogManager.getLogger();
  private Map<String, DatabaseSettings> dbSettings = new ConcurrentHashMap<>();
  private Map<String, Future<JdbcClient>> statusClients = new ConcurrentHashMap<>();
  private final int maxDbPoolSize;

  protected JdbcBasedHandler(int maxDbPoolSize) {
    this.maxDbPoolSize = maxDbPoolSize;
  }

  protected Future<JdbcClient> getClient(String connectorId) {
    return statusClients.computeIfAbsent(connectorId, cId -> createClient(connectorId));
  }

  private Future<JdbcClient> createClient(String connectorId) {
    return dbSettingsForConnector(connectorId)                                                                  //TODO: Check why Maintenance service needs common schema
        .compose(dbSettings -> Future.succeededFuture(new JdbcClient(new PooledDataSources(dbSettings.withSearchPath(List.of("hub.common")))).withQueueing(true)));
  }

  private Future<DatabaseSettings> dbSettingsForConnector(String connectorId) {
    return LegacyHubWebClient.getConnectorConfig(connectorId)
        .compose(connector -> {
          dbSettings.put(connectorId, new DatabaseSettings(connectorId,
              ECPSTool.decryptToMap(CService.configuration.ECPS_PHRASE, ConnectorParameters.fromMap(connector.params).getEcps()))
              .withApplicationName(getApplicationName(connectorId))
              .withDbMaxPoolSize(maxDbPoolSize)
              .withStatementTimeoutSeconds(CService.configuration.DB_STATEMENT_TIMEOUT_IN_S)
              .withDbCheckoutTimeout(CService.configuration.DB_CHECKOUT_TIMEOUT  * 1000)
              .withDbAcquireRetryAttempts(CService.configuration.DB_ACQUIRE_RETRY_ATTEMPTS)
          );
          return Future.succeededFuture(dbSettings.get(connectorId));
        });
  }

  private static String getApplicationName(String clientId) {
    String applicationName = CService.APPLICATION_NAME_PREFIX + Core.START_TIME + "#" + clientId;
    return applicationName.length() > 63 ? applicationName.substring(0, 63) : applicationName;
  }

  /**
   * Use with care. All underlying connections of the client will be closed when removing it.
   * This could have an effect on all queries which are currently in-flight in other threads.
   * @param connectorId
   */
  protected void removeClient(String connectorId) {
    statusClients.remove(connectorId).onSuccess(client -> {
      try {
        client.close();
      }
      catch (Exception e) {
        logger.error(e);
      }
    });
  }

  protected DatabaseSettings getDbSettings(String connectorId) {
    return dbSettings.get(connectorId);
  }
}
