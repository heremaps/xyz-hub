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

package com.here.xyz.httpconnector.config;

import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.task.JdbcBasedHandler;
import com.here.xyz.httpconnector.util.web.LegacyHubWebClient;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.maintenance.ConnectorStatus;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.JdbcClient;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.PARTITION_SIZE;

public class JDBCMaintainer extends JdbcBasedHandler {
    private static final Logger logger = LogManager.getLogger();
    public static final String SPACE_NOT_FOUND_OR_INVALID = "SNFOI";
    public static final String SPACE_MAINTENANCE_ENTRY_NOT_VALID = "SMENV";
    public static final String CONNECTOR_MAINTENANCE_ENTRY_NOT_VALID = "CMENV";
    private static final JDBCMaintainer instance = new JDBCMaintainer();

    private JDBCMaintainer() {
      super(CService.configuration.JOB_DB_POOL_SIZE_PER_MAINTENANCE_CLIENT);
    }

    public static JDBCMaintainer getInstance() {
      return instance;
    }

  public Future<ConnectorStatus> getConnectorStatus(String connectorId) {
      return getClient(connectorId)
          .compose(client -> getConnectorStatus(client, getDbSettings(connectorId)));
  }

  private Future<ConnectorStatus> getConnectorStatus(JdbcClient client, DatabaseSettings dbSettings) {
    String connectorId = dbSettings.getId();
    SQLQuery q = new SQLQuery("select /*maintenance_hint m499#connectorId("+connectorId+")*/ (select row_to_json( prop ) from ( select 'ConnectorStatus' as type, connector_id, initialized, " +
        "extensions, script_versions as \"scriptVersions\", maintenance_status as \"maintenanceStatus\") prop ) " +
        "from xyz_config.db_status where dh_schema = #{schema} AND connector_id = #{connectorId}")
        .withNamedParameter("schema", dbSettings.getSchema())
        .withNamedParameter("connectorId", connectorId);

    return client.run(q, rs -> rs.next() ? rs.getString(1) : null)
        .compose(
            statusJson -> {
              if (statusJson != null) {
                try {
                  return Future.succeededFuture(XyzSerializable.deserialize(statusJson, ConnectorStatus.class));
                }
                catch (Exception e) {
                  return Future.failedFuture(CONNECTOR_MAINTENANCE_ENTRY_NOT_VALID);
                }
              }
              else
                return Future.succeededFuture(new ConnectorStatus().withInitialized(false));
            },
            t -> {
              if (t != null && t instanceof SQLException sqlException && sqlException.getSQLState() != null
                  && sqlException.getSQLState().equals("42P01"))
                //Table Not Exists => DB not initialized
                return Future.succeededFuture(new ConnectorStatus().withInitialized(false));
              else
                return Future.failedFuture(t);
            }
        );
  }


    public Future<Void> purgeOldVersions(String spaceId, Long minTagVersion) {
       return LegacyHubWebClient.getSpace(spaceId)
               .compose(space -> injectEnableHashedSpaceIdIntoParams(space))
               //Load space-config
               .compose(space -> {
                   if(space.getStorage() == null)
                       return Future.failedFuture(SPACE_NOT_FOUND_OR_INVALID);
                   //Load JDBC Client
                    return getClient(space.getStorage().getId())
                       .compose(client -> {
                         DatabaseSettings dbSettings = getDbSettings(space.getStorage().getId());
                         String table = ConnectorParameters.fromMap(space.getStorage().getParams()).isEnableHashedSpaceId()
                               ? Hasher.getHash(spaceId) : spaceId;

                         logger.info("[{}]: Purge old versions {}@{}", spaceId, table, space.getStorage().getId());

                         SQLQuery q = new SQLQuery("SELECT /*maintenance_hint m499#spaceId("+spaceId+")*/ xyz_advanced_delete_changesets(#{schema}, #{table}, #{partitionSize}, #{versionsToKeep}, #{minTagVersion}, #{pw})")
                                 .withNamedParameter("schema", dbSettings.getSchema())
                                 .withNamedParameter("table", table)
                                 .withNamedParameter("partitionSize", PARTITION_SIZE)
                                 .withNamedParameter("versionsToKeep", space.getVersionsToKeep())
                                 .withNamedParameter("minTagVersion", minTagVersion)
                                 .withNamedParameter("pw", dbSettings.getPassword());

                         return client.run(q);
                       });
        });
    }

    private Future<Space> injectEnableHashedSpaceIdIntoParams(Space space) {
        return LegacyHubWebClient.getConnectorConfig(space.getStorage().getId())
                .compose(connector -> {
                    boolean enableHashedSpaceId = connector.params.containsKey("enableHashedSpaceId")
                            ? (boolean) connector.params.get("enableHashedSpaceId") : false;

                    Map<String, Object> params = space.getStorage().getParams() == null ?
                            new HashMap<>() : space.getStorage().getParams() ;
                    params.putIfAbsent("enableHashedSpaceId", enableHashedSpaceId);
                    space.getStorage().setParams(params);

                    return Future.succeededFuture(space);
                });
    }
}
