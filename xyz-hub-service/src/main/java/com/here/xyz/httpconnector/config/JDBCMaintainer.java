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

import static com.here.xyz.psql.DatabaseHandler.PARTITION_SIZE;
import static com.here.xyz.psql.query.ModifySpace.IDX_STATUS_TABLE_FQN;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.task.JdbcBasedHandler;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.Core;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.DatabaseMaintainer;
import com.here.xyz.psql.config.ConnectorParameters;
import com.here.xyz.psql.factory.MaintenanceSQL;
import com.here.xyz.responses.maintenance.ConnectorStatus;
import com.here.xyz.responses.maintenance.SpaceStatus;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.DatabaseSettings;
import com.here.xyz.util.db.JdbcClient;
import com.here.xyz.util.db.SQLQuery;
import io.vertx.core.Future;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.naming.NoPermissionException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JDBCMaintainer extends JdbcBasedHandler {
    private static final Logger logger = LogManager.getLogger();
    private static final String[] extensionList = new String[]{"postgis","postgis_topology","tsm_system_rows","dblink","aws_s3"};
    private static final String localExtScript = "/xyz_ext.sql";
    private static final String localH3Script = "/h3Core.sql";
    public static final String SPACE_NOT_FOUND_OR_INVALID = "SNFOI";
    private static final String SPACE_MAINTENANCE_ENTRY_NOT_PRESENT = "SMENP";
    public static final String SPACE_MAINTENANCE_ENTRY_NOT_VALID = "SMENV";
    public static final String CONNECTOR_MAINTENANCE_ENTRY_NOT_PRESENT = "CMENV";
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

  public Future<SpaceStatus> getMaintenanceStatusOfSpace(String spaceId) {
      return HubWebClient.getSpace(spaceId)
              .compose(space -> injectEnableHashedSpaceIdIntoParams(space))
              //Load space-config
              .compose(space -> {
                  if(space.getStorage() == null)
                      return Future.failedFuture(SPACE_NOT_FOUND_OR_INVALID);

                  //Load JDBC Client
                  return getClient(space.getStorage().getId())
                          .compose(
                              client -> {
                                DatabaseSettings dbSettings = getDbSettings(space.getStorage().getId());
                                String table = ConnectorParameters.fromMap(space.getStorage().getParams()).isEnableHashedSpaceId() ? Hasher.getHash(spaceId) : spaceId;
                                logger.info("[{}]: Get maintenanceStatus {}@{}", spaceId, table, space.getStorage().getId());

                                SQLQuery statusQuery = new SQLQuery("select /*maintenance_hint m499#spaceId("+spaceId+")*/ (" +
                                        "select row_to_json( status ) " +
                                        "   from (" +
                                        "          select 'SpaceStatus' as type, " +
                                        "           (extract(epoch from runts AT TIME ZONE 'UTC')*1000)::BIGINT as runts, " +
                                        "           spaceid, " +
                                        "           idx_creation_finished as \"idxCreationFinished\"," +
                                        "           count, " +
                                        "           auto_indexing as \"autoIndexing\", " +
                                        "           idx_available as \"idxAvailable\", " +
                                        "           idx_manual as \"idxManual\", " +
                                        "            idx_proposals as \"idxProposals\"," +
                                        "            prop_stat as \"propStats\"" +
                                        "   ) status " +
                                        ") from "+ IDX_STATUS_TABLE_FQN
                                        +" where schem = #{schema} and spaceid = #{spaceId};")
                                        .withNamedParameter("schema", dbSettings.getSchema())
                                        .withNamedParameter("spaceId", table);

                                return client.run(statusQuery, rs -> rs.next() ? rs.getString(1) : null)
                                    .compose(statusJson -> {
                                      if (statusJson == null)
                                        return Future.failedFuture(SPACE_MAINTENANCE_ENTRY_NOT_PRESENT);
                                      try {
                                        return Future.succeededFuture(XyzSerializable.deserialize(statusJson, SpaceStatus.class));
                                      }
                                      catch (JsonProcessingException e) {
                                        return Future.failedFuture("Can not serialize SpaceStatus");
                                      }
                                    });
                              });
              });
  }

  public Future<ConnectorStatus> initializeOrUpdateDatabase(String connectorId) {
    return getClient(connectorId)
        .compose(client -> initializeOrUpdateDatabase(client, getDbSettings(connectorId)));
  }

    private Future<ConnectorStatus> initializeOrUpdateDatabase(JdbcClient client, DatabaseSettings dbSettings) {
      String connectorId = dbSettings.getId();
        return Future.succeededFuture()
            .compose(v -> {
                SQLQuery hasPermissionsQuery = new SQLQuery("select has_database_privilege(#{user}, #{db}, 'CREATE')")
                        .withNamedParameter("user", dbSettings.getUser())
                        .withNamedParameter("db", dbSettings.getDb());

                return client.run(hasPermissionsQuery,
                        rs -> {
                          if (rs.next() && rs.getBoolean(1))
                            return true;
                          return false;
                        })
                    .compose(hasPermission -> hasPermission
                        ? Future.succeededFuture(dbSettings) : Future.failedFuture(new NoPermissionException("")));
            })
            .compose(v -> {
                logger.info("{}: Create required Extensions..", connectorId);
                SQLQuery q = new SQLQuery(MaintenanceSQL.generateMandatoryExtensionSQL());

                return client.write(q).mapEmpty();
            })
                .compose(v -> {
                logger.info("{}: Create required Main-Schema..", connectorId);
                SQLQuery q = new SQLQuery("CREATE SCHEMA IF NOT EXISTS \"" + dbSettings.getSchema() + "\";");

                return client.write(q).mapEmpty();
            })
            .compose(v -> {
                logger.info("{}: Create required Config-Schema and System-Tables..", connectorId);
                SQLQuery q = new SQLQuery(MaintenanceSQL.configSchemaAndSystemTablesSQL);

                return client.write(q).mapEmpty();
            })
            .compose(v -> {
                logger.info("{}: Create required IDX-Table..", connectorId);
                SQLQuery q = new SQLQuery(MaintenanceSQL.createIDXTable);

                return client.write(q).mapEmpty();
            })
            .compose(v -> {
                logger.info("{}: Add initial IDX Entry", connectorId);
                SQLQuery q = new SQLQuery(MaintenanceSQL.createIDXInitEntry);

                return client.write(q).mapEmpty();
            })
            .compose(v -> {
                logger.info("{}: Create required Space-Meta-Table..", connectorId);
                SQLQuery q = new SQLQuery(MaintenanceSQL.createDbStatusTable);

                return client.write(q).mapEmpty();
            })
            .compose(v -> {
                logger.info("{}: Create required Tag-Table..", connectorId);
                SQLQuery q = new SQLQuery(MaintenanceSQL.createSpaceMetaTable);

                return client.write(q).mapEmpty();
            })
            .compose(v -> {
                logger.info("{}: Create required Subscription-Table..", connectorId);
                SQLQuery q = new SQLQuery(MaintenanceSQL.createTagTable);

                return client.write(q).mapEmpty();
            })
            .compose(v -> {
                try {
                    logger.info("{}: Apply EXT Script \"{}\" ..", localExtScript);
                    String content = DatabaseMaintainer.readResource(localExtScript);
                    SQLQuery q = new SQLQuery(content);

                    return client.write(q).mapEmpty();
                }
                catch (IOException e) {
                    return Future.failedFuture("Can not read " + localExtScript);
                }
            })
            .compose(v -> {
                try {
                    logger.info("{}: Apply H3 Script \"{}\" ..", localExtScript);

                    String content = DatabaseMaintainer.readResource(localH3Script);
                    logger.info("{}: Apply Script \"{}\" ..", localH3Script);
                    SQLQuery q = new SQLQuery(content);

                    return client.write(q).mapEmpty()
                        .compose(v2 -> {
                            //Reset H3 Search-Path
                            removeClient(connectorId);
                            //Retrieve a new client
                            return getClient(connectorId);
                        });
                }
                catch (IOException e) {
                    return Future.failedFuture("Can not read " + localExtScript);
                }
            })
            .compose(newClient -> {
                logger.info("{}: Mark db as initialized in db-status table..", connectorId);

                SQLQuery addInitializationEntry = new SQLQuery("INSERT INTO xyz_config.db_status as a (dh_schema, connector_id, initialized, extensions, script_versions, maintenance_status) " +
                        "VALUES (#{schema}, #{connectorId}, #{initialized}, #{extensions}, '{\"ext\": " + DatabaseMaintainer.XYZ_EXT_VERSION + ", \"h3\": " + DatabaseMaintainer.H3_CORE_VERSION + "}'::jsonb, #{maintenanceStatus}::jsonb)"
                        + "ON CONFLICT (dh_schema,connector_id) DO "
                        + "UPDATE SET extensions = #{extensions},"
                        + "    		  script_versions = '{\"ext\": " + DatabaseMaintainer.XYZ_EXT_VERSION + ", \"h3\": " + DatabaseMaintainer.H3_CORE_VERSION + "}'::jsonb,"
                        + "    		  initialized = #{initialized}"
                        + "		WHERE a.dh_schema = #{schema} AND a.connector_id = #{connectorId}")
                        .withNamedParameter("schema", dbSettings.getSchema())
                        .withNamedParameter("connectorId", connectorId)
                        .withNamedParameter("initialized", true)
                        .withNamedParameter("extensions", extensionList)
                        .withNamedParameter("maintenanceStatus", null);

                return newClient.write(addInitializationEntry)
                    .map(v -> new ConnectorStatus().withInitialized(true));
            });
    }

    public Future<Void>  maintainIndices(String connectorId, boolean autoIndexing, boolean force) {
        return getClient(connectorId).compose(
                client -> {
                  DatabaseSettings dbSettings = getDbSettings(connectorId);
                    String[] dbUser = new String[]{"wikvaya", dbSettings.getUser()};
                    String maintenanceJobId = "" + Core.currentTimeMillis();
                    int mode = autoIndexing == true ? 2 : 0;

                    //Check Status of idx_
                    SQLQuery q = new SQLQuery(MaintenanceSQL.checkIDXStatus);

                    return client.run(q,
                            rs -> {
                              if (rs.next())
                                return rs.getInt(1);
                              return -1;
                            })
                        .<Future<Void>>compose(status -> {
                          if (status == 16) {
                            logger.info("{}: Indexing is disabled database wide! ", connectorId);
                            return Future.failedFuture("Indexing is disabled database wide!");
                          }
                          return Future.succeededFuture();
                        })
                        .compose(v -> {

                          SQLQuery updateDBStatus = (force == false ?
                                  new SQLQuery("UPDATE /*maintenance_hint m499#connectorId("+connectorId+")*/ xyz_config.db_status SET maintenance_status=" +
                                          " CASE" +
                                          "  WHEN maintenance_status IS NULL THEN" +
                                          "       jsonb_set('{}'::jsonb || '{\"AUTO_INDEXING\":{\"maintenanceRunning\" : []}}', '{AUTO_INDEXING,maintenanceRunning}',  #{maintenanceJobId}::jsonb || '[]'::jsonb)" +
                                          "  WHEN maintenance_status->'AUTO_INDEXING'->'maintenanceRunning' IS NULL THEN" +
                                          "       jsonb_set(maintenance_status || '{\"AUTO_INDEXING\":{\"maintenanceRunning\" : []}}', '{AUTO_INDEXING,maintenanceRunning}', #{maintenanceJobId}::jsonb || '[]'::jsonb)" +
                                          "   ELSE" +
                                          "       jsonb_set(maintenance_status,'{AUTO_INDEXING,maintenanceRunning,999}',#{maintenanceJobId}::jsonb)" +
                                          " END"
                                          + "		WHERE dh_schema = #{schema} AND connector_id = #{connectorId}")
                                  : new SQLQuery("UPDATE xyz_config.db_status SET maintenance_status=" +
                                  " jsonb_set(maintenance_status,'{AUTO_INDEXING,maintenanceRunning}', #{maintenanceJobId}::jsonb ||  '[]'::jsonb) " +
                                  "		WHERE dh_schema=#{schema} AND connector_id=#{connectorId}"))
                                  .withNamedParameter("maintenanceJobId", maintenanceJobId)
                                  .withNamedParameter("schema", dbSettings.getSchema())
                                  .withNamedParameter("connectorId", connectorId);

                          return client.write(updateDBStatus).<Future<Void>>mapEmpty();
                        }).compose(v -> {
                          logger.info("{}: Start Indexing.. AutoIndexing={}", connectorId, autoIndexing);

                          SQLQuery triggerIndexing = new SQLQuery("SELECT  /*maintenance_hint m499#connectorId("+connectorId+")*/ xyz_create_idxs(#{schema}, #{limit}, #{offset}, #{mode}, #{user})")
                                  .withNamedParameter("schema", dbSettings.getSchema())
                                  .withNamedParameter("limit", 100)
                                  .withNamedParameter("offset", 0)
                                  .withNamedParameter("mode", mode)
                                  .withNamedParameter("user", dbUser);

                          return client.run(triggerIndexing).mapEmpty();
                        })
                        .compose(v -> {
                          SQLQuery updateDBStatus = new SQLQuery("UPDATE /*maintenance_hint m499#connectorId("+connectorId+")*/ xyz_config.db_status SET maintenance_status =  "+
                                  "jsonb_set( jsonb_set(maintenance_status,'{AUTO_INDEXING,maintainedAt}'::text[], #{maintenanceJobId}::jsonb),'{AUTO_INDEXING,maintenanceRunning}'," +
                                  "   COALESCE((select jsonb_agg(jsonb_array_elements) from jsonb_array_elements(maintenance_status->'AUTO_INDEXING'->'maintenanceRunning')" +
                                  "           where jsonb_array_elements != #{maintenanceJobId}::jsonb), '[]'::jsonb)" +
                                  "    )" +
                                  "    WHERE dh_schema = #{schema} AND connector_id = #{connectorId}")
                                  .withNamedParameter("maintenanceJobId", maintenanceJobId)
                                  .withNamedParameter("schema", dbSettings.getSchema())
                                  .withNamedParameter("connectorId", connectorId);

                          logger.info("{}: Mark Indexing as finished", connectorId);

                          return client.write(updateDBStatus).mapEmpty();
                        });
                }
        );
    }

    public Future<Void> maintainSpace(String spaceId) {
        //Load space-config
        return HubWebClient.getSpace(spaceId)
                .compose(space -> injectEnableHashedSpaceIdIntoParams(space))
                .compose(space -> {
                    //Load JDBC Client
                    return getClient(space.getStorage().getId())
                        .compose(client -> {
                          DatabaseSettings dbSettings = getDbSettings(space.getStorage().getId());
                          String table = ConnectorParameters.fromMap(space.getStorage().getParams()).isEnableHashedSpaceId() ? Hasher.getHash(spaceId) : spaceId;

                          return Future.succeededFuture()
                              .compose(v -> {
                                SQLQuery updateIDXEntry = new SQLQuery("UPDATE /*maintenance_hint m499#spaceId("+spaceId+")*/" + IDX_STATUS_TABLE_FQN
                                    + " SET idx_creation_finished = null "
                                    + " WHERE schem = #{schema} AND spaceid = #{spaceId}")
                                    .withNamedParameter("schema", dbSettings.getSchema())
                                    .withNamedParameter("spaceId", table);
                                logger.info("[{}]: Set idx_creation_finished=NULL {}@{}", spaceId, table, space.getStorage().getId());

                                return client.write(updateIDXEntry).mapEmpty();
                              })
                              .compose(v -> {
                                logger.info("[{}]: Maintain Space {}@{}", spaceId, table, space.getStorage().getId());
                                SQLQuery maintainSpace = new SQLQuery("select /*maintenance_hint m499#spaceId("+spaceId+")*/ xyz_maintain_idxs_for_space(#{schema}, #{spaceId})")
                                    .withNamedParameter("schema", dbSettings.getSchema())
                                    .withNamedParameter("spaceId", table);

                                return client.run(maintainSpace);
                              });
                        });
                });
    }

    public Future<Void> purgeOldVersions(String spaceId, Long minTagVersion) {
       return HubWebClient.getSpace(spaceId)
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
        return HubWebClient.getConnectorConfig(space.getStorage().getId())
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
