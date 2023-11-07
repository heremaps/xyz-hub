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
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.Core;
import com.here.xyz.psql.DatabaseMaintainer;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.factory.MaintenanceSQL;
import com.here.xyz.responses.maintenance.ConnectorStatus;
import com.here.xyz.responses.maintenance.SpaceStatus;
import com.here.xyz.util.Hasher;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.pgclient.PgException;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.impl.ArrayTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.NoPermissionException;
import java.io.IOException;

import static com.here.xyz.psql.DatabaseHandler.PARTITION_SIZE;
import static com.here.xyz.psql.query.ModifySpace.IDX_STATUS_TABLE_FQN;

public class JDBCMaintainer extends JDBCClients{
    private static final Logger logger = LogManager.getLogger();
    private static final String[] extensionList = new String[]{"postgis","postgis_topology","tsm_system_rows","dblink","aws_s3"};
    private static final String localExtScript = "/xyz_ext.sql";
    private static final String localH3Script = "/h3Core.sql";
    public static final String SPACE_NOT_FOUND_OR_INVALID = "SNFOI";
    public static final String SPACE_MAINTENANCE_ENTRY_NOT_PRESENT = "SMENP";
    public static final String SPACE_MAINTENANCE_ENTRY_NOT_VALID = "SMENV";
    public static final String CONNECTOR_MAINTENANCE_ENTRY_NOT_PRESENT = "CMENV";
    public static final String CONNECTOR_MAINTENANCE_ENTRY_NOT_VALID = "CMENV";

    public static Future<ConnectorStatus> getConnectorStatus(String connectorId) {
       return addClientsIfRequired(connectorId, false)
               .compose(dbSettings -> {
                    SQLQuery q = new SQLQuery("select /*maintenance_hint m499#connectorId("+connectorId+")*/ (select row_to_json( prop ) from ( select 'ConnectorStatus' as type, connector_id, initialized, " +
                            "extensions, script_versions as \"scriptVersions\", maintenance_status as \"maintenanceStatus\") prop ) " +
                            "from xyz_config.db_status where dh_schema = #{schema} AND connector_id = #{connectorId}")
                            .withNamedParameter("schema", dbSettings.getSchema())
                            .withNamedParameter("connectorId", connectorId);

                    SQLQuery.substituteAndUseDollarSyntax(q);
                    Promise<ConnectorStatus> p = Promise.promise();

                    getClient(connectorId, false)
                            .preparedQuery(q.text())
                            .execute(new ArrayTuple(q.parameters()))
                            .onComplete(res -> {
                                if(res.succeeded()) {
                                    RowIterator<Row> iterator = res.result().iterator();
                                    if (iterator.hasNext()) {
                                        try {
                                            p.complete(XyzSerializable.deserialize(iterator.next().getJsonObject(0).toString(), ConnectorStatus.class));
                                        } catch (Exception e) {
                                            p.fail(CONNECTOR_MAINTENANCE_ENTRY_NOT_VALID);
                                        }
                                    }else
                                        p.complete(new ConnectorStatus().withInitialized(false));
                                }else{
                                    if(res.cause() != null && res.cause() instanceof PgException
                                            && ((PgException)res.cause()).getSqlState() != null && ((PgException)res.cause()).getSqlState().equals("42P01"))
                                        /** Table Not Exists => DB not initialized */
                                        p.complete(new ConnectorStatus().withInitialized(false));
                                    else
                                        p.fail(res.cause());
                                }
                            }).onFailure(f -> {
                                 p.fail(f);
                            });
                    return p.future();
                }
        );
    }

    public static Future<SpaceStatus> getMaintenanceStatusOfSpace(String spaceId) {
        return HubWebClient.getSpace(spaceId)
                /** Load space-config */
                .compose(space -> {
                    if(space.getStorage() == null)
                        return Future.failedFuture(SPACE_NOT_FOUND_OR_INVALID);

                    /** Load JDBC Client */
                    return addClientsIfRequired(space.getStorage().getId(), false)
                            .compose(
                                dbSettings -> {
                                    String table = spaceId;
                                    if(dbSettings.isEnabledHashSpaceId())
                                        table = Hasher.getHash(spaceId);

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

                                    SQLQuery.substituteAndUseDollarSyntax(statusQuery);

                                    Promise<SpaceStatus> p = Promise.promise();

                                    getClient(space.getStorage().getId(), false)
                                            .preparedQuery(statusQuery.text())
                                            .execute(new ArrayTuple(statusQuery.parameters()))
                                            .onComplete(res -> {
                                                RowIterator<Row> iterator = res.result().iterator();
                                                if (iterator.hasNext()) {
                                                    try {
                                                        p.complete(XyzSerializable.deserialize(iterator.next().getJson(0).toString(), SpaceStatus.class));
                                                    } catch (Exception e) {
                                                        p.fail("Cant serialize SpaceStatus");
                                                        p.fail(SPACE_MAINTENANCE_ENTRY_NOT_VALID);
                                                    }
                                                }else {
                                                    p.fail(SPACE_MAINTENANCE_ENTRY_NOT_PRESENT);
                                                }
                                            });

                                    return p.future();
                                });
                });
    }

    public static Future<ConnectorStatus> initializeOrUpdateDatabase(String connectorId) {
        return addClientsIfRequired(connectorId, false).compose(
                dbSettings -> {
                    SQLQuery hasPermissionsQuery = new SQLQuery("select has_database_privilege(#{user}, #{db}, 'CREATE')")
                            .withNamedParameter("user", dbSettings.getUser())
                            .withNamedParameter("db", dbSettings.getDb());

                    SQLQuery.substituteAndUseDollarSyntax(hasPermissionsQuery);

                    return getClient(connectorId, false)
                            .preparedQuery(hasPermissionsQuery.text())
                            .execute(new ArrayTuple(hasPermissionsQuery.parameters()))
                            .compose(res -> {
                                if(res.iterator().hasNext() && res.iterator().next().getBoolean(0))
                                    return Future.succeededFuture(dbSettings);
                                return Future.failedFuture(new NoPermissionException(""));
                            });
                }
        ).compose(dbSettings -> {
            logger.info("{}: Create required Extensions..", connectorId);
            SQLQuery q = new SQLQuery(MaintenanceSQL.generateMandatoryExtensionSQL());

            return getClient(connectorId, false)
                    .query(q.text())
                    .execute()
                    .compose(t -> Future.succeededFuture(dbSettings));
        }).compose(dbSettings -> {
            logger.info("{}: Create required Main-Schema..", connectorId);
            SQLQuery q = new SQLQuery("CREATE SCHEMA IF NOT EXISTS \"" + dbSettings.getSchema() + "\";");

            return getClient(connectorId, false)
                    .query(q.text())
                    .execute()
                    .compose(t -> Future.succeededFuture(dbSettings));
        }).compose(dbSettings-> {
            logger.info("{}: Create required Config-Schema and System-Tables..", connectorId);
            SQLQuery q = new SQLQuery(MaintenanceSQL.configSchemaAndSystemTablesSQL);

            return getClient(connectorId, false)
                    .query(q.text())
                    .execute()
                    .compose(t -> Future.succeededFuture(dbSettings));
        }).compose(dbSettings -> {
            logger.info("{}: Create required IDX-Table..", connectorId);
            SQLQuery q = new SQLQuery(MaintenanceSQL.createIDXTable);

            return getClient(connectorId, false)
                    .query(q.text())
                    .execute()
                    .compose(t -> Future.succeededFuture(dbSettings));
        }).compose(dbSettings -> {
            logger.info("{}: Add initial IDX Entry", connectorId);
            SQLQuery q = new SQLQuery(MaintenanceSQL.createIDXInitEntry);

            return getClient(connectorId, false)
                    .query(q.text())
                    .execute()
                    .compose(t -> Future.succeededFuture(dbSettings));
        }).compose(dbSettings -> {
            logger.info("{}: Create required Space-Meta-Table..", connectorId);
            SQLQuery q = new SQLQuery(MaintenanceSQL.createDbStatusTable);

            return getClient(connectorId, false)
                    .query(q.text())
                    .execute()
                    .compose(t -> Future.succeededFuture(dbSettings));
        }).compose(dbSettings -> {
            logger.info("{}: Create required Tag-Table..", connectorId);
            SQLQuery q = new SQLQuery(MaintenanceSQL.createSpaceMetaTable);

            return getClient(connectorId, false)
                    .query(q.text())
                    .execute()
                    .compose(t -> Future.succeededFuture(dbSettings));
        }).compose(dbSettings -> {
            logger.info("{}: Create required Subscription-Table..", connectorId);
            SQLQuery q = new SQLQuery(MaintenanceSQL.createTagTable);

            return getClient(connectorId, false)
                    .query(q.text())
                    .execute()
                    .compose(t -> Future.succeededFuture(dbSettings));
        }).compose(dbSettings -> {
            try {
                logger.info("{}: Apply EXT Script \"{}\" ..", localExtScript);
                String content = DatabaseMaintainer.readResource(localExtScript);
                SQLQuery q = new SQLQuery(content);

                return getClient(connectorId, false)
                        .query(q.text())
                        .execute()
                        .compose(t -> Future.succeededFuture(dbSettings));
            } catch (IOException e) {
                return Future.failedFuture("Cant read "+localExtScript);
            }
        }).compose(dbSettings -> {
            try {
                logger.info("{}: Apply H3 Script \"{}\" ..", localExtScript);

                String content = DatabaseMaintainer.readResource(localH3Script);
                logger.info("{}: Apply Script \"{}\" ..", localH3Script);
                SQLQuery q = new SQLQuery(content);

                return getClient(connectorId, false)
                        .query(q.text())
                        .execute()
                        .compose(t -> {
                            //reset H3 Search-Path
                            removeClients(connectorId);
                            return Future.succeededFuture(dbSettings);
                        })
                        .compose(t -> addClientsIfRequired(connectorId, false));
            } catch (IOException e) {
                return Future.failedFuture("Cant read "+localExtScript);
            }
        }).compose(dbSettings -> {
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

            SQLQuery.substituteAndUseDollarSyntax(addInitializationEntry);

            return getClient(connectorId, false)
                    .preparedQuery(addInitializationEntry.text())
                    .execute(new ArrayTuple(addInitializationEntry.parameters()))
                    .compose(f -> Future.succeededFuture(new ConnectorStatus().withInitialized(true)));
        });
    }

    public static Future<Void>  maintainIndices(String connectorId, boolean autoIndexing, boolean force) {
        return addClientsIfRequired(connectorId, false).compose(
                dbSettings -> {

                    String[] dbUser = new String[]{"wikvaya", dbSettings.getUser()};
                    String maintenanceJobId = ""+ Core.currentTimeMillis();
                    int mode = autoIndexing == true ? 2 : 0;

                    /** Check Status of idx_ */
                    SQLQuery q = new SQLQuery(MaintenanceSQL.checkIDXStatus);
                    SQLQuery.substituteAndUseDollarSyntax(q);

                    return getClient(connectorId, false)
                            .preparedQuery(q.text())
                            .execute(new ArrayTuple(q.parameters()))
                            .compose(row -> {
                                Row res = row.iterator().next();
                                if (res != null) {
                                    int status = res.getInteger(0);
                                    if(status == 16){
                                        logger.info("{}: Indexing is disabled database wide! ", connectorId);
                                        return Future.failedFuture("Indexing is disabled database wide!");
                                    }
                                }
                                return Future.succeededFuture();
                            }).compose(
                                f -> {
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

                                    SQLQuery.substituteAndUseDollarSyntax(updateDBStatus);

                                    return getClient(connectorId, false)
                                            .preparedQuery(updateDBStatus.text())
                                            .execute(new ArrayTuple(updateDBStatus.parameters()));
                                }
                            ).compose( f -> {
                                logger.info("{}: Start Indexing.. AutoIndexing={}", connectorId, autoIndexing);

                                SQLQuery triggerIndexing = new SQLQuery("SELECT  /*maintenance_hint m499#connectorId("+connectorId+")*/ xyz_create_idxs(#{schema}, #{limit}, #{offset}, #{mode}, #{user})")
                                        .withNamedParameter("schema", dbSettings.getSchema())
                                        .withNamedParameter("limit", 100)
                                        .withNamedParameter("offset", 0)
                                        .withNamedParameter("mode", mode)
                                        .withNamedParameter("user", dbUser);

                                SQLQuery.substituteAndUseDollarSyntax(triggerIndexing);

                                return getClient(connectorId, false)
                                        .preparedQuery(triggerIndexing.text())
                                        .execute(new ArrayTuple(triggerIndexing.parameters()));
                                }
                            ).compose( f -> {

                                SQLQuery updateDBStatus = new SQLQuery("UPDATE /*maintenance_hint m499#connectorId("+connectorId+")*/ xyz_config.db_status SET maintenance_status =  "+
                                        "jsonb_set( jsonb_set(maintenance_status,'{AUTO_INDEXING,maintainedAt}'::text[], #{maintenanceJobId}::jsonb),'{AUTO_INDEXING,maintenanceRunning}'," +
                                        "   COALESCE((select jsonb_agg(jsonb_array_elements) from jsonb_array_elements(maintenance_status->'AUTO_INDEXING'->'maintenanceRunning')" +
                                        "           where jsonb_array_elements != #{maintenanceJobId}::jsonb), '[]'::jsonb)" +
                                        "    )" +
                                        "    WHERE dh_schema = #{schema} AND connector_id = #{connectorId}")
                                        .withNamedParameter("maintenanceJobId", maintenanceJobId)
                                        .withNamedParameter("schema", dbSettings.getSchema())
                                        .withNamedParameter("connectorId", connectorId);

                                SQLQuery.substituteAndUseDollarSyntax(updateDBStatus);

                                logger.info("{}: Mark Indexing as finished", connectorId);

                                return getClient(connectorId, false)
                                        .preparedQuery(updateDBStatus.text())
                                        .execute(new ArrayTuple(updateDBStatus.parameters()))
                                        .map(t -> null);
                            });
                }
        );
    }

    public static Future<Void> maintainSpace(String spaceId) {
        return HubWebClient.getSpace(spaceId)
                /** Load space-config */
                .compose(space -> {
                    /** Load JDBC Client */
                    return addClientsIfRequired(space.getStorage().getId(), false)
                            .compose(dbSettings -> {
                                String table = spaceId;
                                if(dbSettings.isEnabledHashSpaceId())
                                    table = Hasher.getHash(spaceId);

                                SQLQuery updateIDXEntry = new SQLQuery("UPDATE /*maintenance_hint m499#spaceId("+spaceId+")*/" + IDX_STATUS_TABLE_FQN
                                        + " SET idx_creation_finished = null "
                                        + " WHERE schem = #{schema} AND spaceid = #{spaceId}")
                                        .withNamedParameter("schema", dbSettings.getSchema())
                                        .withNamedParameter("spaceId", table);
                                logger.info("[{}]: Set idx_creation_finished=NULL {}@{}", spaceId, table, space.getStorage().getId());
                                SQLQuery.substituteAndUseDollarSyntax(updateIDXEntry);

                                return getClient(space.getStorage().getId(), false)
                                        .preparedQuery(updateIDXEntry.text())
                                        .execute(new ArrayTuple(updateIDXEntry.parameters()))
                                        .compose(f -> Future.succeededFuture(dbSettings));
                            }).compose(dbSettings -> {
                                String table = spaceId;
                                if(dbSettings.isEnabledHashSpaceId())
                                    table = Hasher.getHash(spaceId);

                                logger.info("[{}]: Maintain Space {}@{}", spaceId, table, space.getStorage().getId());
                                SQLQuery maintainSpace = new SQLQuery("select /*maintenance_hint m499#spaceId("+spaceId+")*/ xyz_maintain_idxs_for_space(#{schema}, #{spaceId})")
                                        .withNamedParameter("schema", dbSettings.getSchema())
                                        .withNamedParameter("spaceId", table);
                                SQLQuery.substituteAndUseDollarSyntax(maintainSpace);

                                return getClient(space.getStorage().getId(), false)
                                        .preparedQuery(maintainSpace.text())
                                        .execute(new ArrayTuple(maintainSpace.parameters()))
                                        .map(f -> null);
                            });
                });
    }

    public  static Future<Void> purgeOldVersions(String spaceId, Long minTagVersion) {

       return HubWebClient.getSpace(spaceId)
               /** Load space-config */
               .compose(space -> {
                   if(space.getStorage() == null)
                       return Future.failedFuture(SPACE_NOT_FOUND_OR_INVALID);
                   /** Load JDBC Client */
                    return addClientsIfRequired(space.getStorage().getId(), false)
                       .compose(dbSettings -> {
                           String table = spaceId;
                           if(dbSettings.isEnabledHashSpaceId())
                               table = Hasher.getHash(spaceId);
                           logger.info("[{}]: Purge old versions {}@{}", spaceId, table, space.getStorage().getId());

                           SQLQuery q = new SQLQuery("SELECT /*maintenance_hint m499#spaceId("+spaceId+")*/ xyz_advanced_delete_changesets(#{schema}, #{table}, #{partitionSize}, #{versionsToKeep}, #{minTagVersion}, #{pw})")
                                   .withNamedParameter("schema", dbSettings.getSchema())
                                   .withNamedParameter("table", table)
                                   .withNamedParameter("partitionSize", PARTITION_SIZE)
                                   .withNamedParameter("versionsToKeep", space.getVersionsToKeep())
                                   .withNamedParameter("minTagVersion", minTagVersion)
                                   .withNamedParameter("pw", dbSettings.getPassword());

                           SQLQuery.substituteAndUseDollarSyntax(q);
                           return getClient(space.getStorage().getId(), false)
                                   .preparedQuery(q.text())
                                   .execute(new ArrayTuple(q.parameters()))
                                   .map(f -> null);
                       });
        });
    }
}
