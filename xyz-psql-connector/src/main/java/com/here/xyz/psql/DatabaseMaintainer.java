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

package com.here.xyz.psql;

import static com.here.xyz.psql.query.ModifySpace.XYZ_CONFIG_SCHEMA;

import com.here.xyz.connectors.AbstractConnectorHandler.TraceItem;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.factory.MaintenanceSQL;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.stream.Collectors;

public class DatabaseMaintainer {
    private static final Logger logger = LogManager.getLogger();

    /** Is used to check against xyz_ext_version() */
    public static final int XYZ_EXT_VERSION = 167;

    public static final int H3_CORE_VERSION = 108;

    private DataSource dataSource;
    private PSQLConfig config;

    private final RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(1 * 1000 )
            .setConnectionRequestTimeout(1 * 1000)
            .setSocketTimeout(1 * 1000)
            .build();

    public DatabaseMaintainer(DataSource dataSource, PSQLConfig config){
        this.dataSource = dataSource;
        this.config = config;
    }

    public synchronized void run(TraceItem traceItem) {
        final boolean hasPropertySearch = config.getConnectorParams().isPropertySearch();
        final boolean autoIndexing = config.getConnectorParams().isAutoIndexing();

        if(config.getMaintenanceEndpoint() != null){
            try (CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()){
                HttpPost request = new HttpPost(config.getMaintenanceEndpoint()
                        +"/maintain/indices?connectorId="+traceItem.getConnectorId()
                        +"&ecps="+config.getEcps()+"&autoIndexing="+autoIndexing
                );

                HttpResponse response = client.execute(request);
                if(response.getStatusLine().getStatusCode() == 405){
                    logger.warn("{} Database not initialized!",traceItem);
                    request = new HttpPost(config.getMaintenanceEndpoint()
                            +"/initialization?connectorId="+traceItem.getConnectorId()
                            +"&ecps="+config.getEcps()+"&autoIndexing="+autoIndexing);
                    response = client.execute(request);
                    if(response.getStatusLine().getStatusCode() >= 400){
                        logger.warn("{} Could not initialize Database! {}",traceItem, EntityUtils.toString(response.getEntity()));
                    }
                } else if(response.getStatusLine().getStatusCode() == 409) {
                    logger.info("{} Maintenance already running! {}", traceItem, EntityUtils.toString(response.getEntity()));
                } else if(response.getStatusLine().getStatusCode() >= 400) {
                    logger.warn("{} Could not maintain Database! {}", traceItem, EntityUtils.toString(response.getEntity()));
                }
            }catch (SocketTimeoutException | ConnectTimeoutException e){
                logger.info("{} Do not further wait for a response {}",traceItem,e);
            }catch (Exception e){
                logger.error("{} Could not maintain Database! {}",traceItem,e);
            }
        }else {
            /** Check if all required extensions, schemas, tables and functions are present  */
            this.initialDBSetup(traceItem, autoIndexing, hasPropertySearch);

            if (hasPropertySearch) {
                /** Trigger missing Index Maintenance (On-Demand & Auto-Indexing) */
                this.triggerIndexing(traceItem, autoIndexing);
            }
        }
    }

    private synchronized void initialDBSetup(TraceItem traceItem, boolean autoIndexing, boolean hasPropertySearch){
        boolean userHasCreatePermissions = false;

        try (final Connection connection = dataSource.getConnection()) {
            boolean functionsUpToDate = false;

            /** Check if database is prepared to work with PSQL Connector. Therefore its needed to check Extensions, Schemas, Tables and Functions.*/
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(MaintenanceSQL.generateCheckExtensionsSQL(hasPropertySearch, config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getDb()));

            if (rs.next()) {
                userHasCreatePermissions = rs.getBoolean("has_create_permissions");
                if (!rs.getBoolean("all_ext_av")) {
                    /** Create Missing IDX_Maintenance Table */
                    if (userHasCreatePermissions) {
                        stmt.execute(MaintenanceSQL.generateMandatoryExtensionSQL());
                    } else {
                        logger.error("{} User permissions missing! Not able to create missing Extensions on database: {}@{} / {}. Installed Extension are: {}",
                                traceItem, config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getDb(), rs.getString("ext_av"));
                        /** Cannot proceed without extensions!
                         * postgis,postgis_topology -> provides all GIS functions which are essential!
                         * tsm_system_rows -> Is used for generating statistics
                         * dblink -> Is used for Auto+On-Demand Indexing
                         * Without */
                        return;
                    }
                }
            }

            stmt = connection.createStatement();
            /** Find Missing Schemas and check if IDX_Maintenance Table is available */
            rs = stmt.executeQuery(MaintenanceSQL.generateCheckSchemasAndIdxTableSQL(config.getDatabaseSettings().getSchema()));

            if (rs.next()) {
                final boolean mainSchema = rs.getBoolean("main_schema");
                boolean configSchema = rs.getBoolean("config_schema");
                final boolean idx_table = rs.getBoolean("idx_table");
                final boolean db_status_table = rs.getBoolean("db_status_table");
                final boolean space_meta_table = rs.getBoolean("space_meta_table");
                final boolean tag_table = rs.getBoolean("tag_table");
                final boolean subscription_table = rs.getBoolean("subscription_table");

                try {
                    // Set the default compression algorithm
                    stmt.execute("SET default_toast_compression=lz4;");

                    /** Create Missing Schemas */
                    if (!mainSchema) {
                        logger.debug("{} Create missing Schema {} on database: {} / {}@{}", traceItem, config.getDatabaseSettings().getSchema(), config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost());
                        stmt.execute(MaintenanceSQL.generateMainSchemaSQL(config.getDatabaseSettings().getSchema()));
                    }

                    if (!configSchema && hasPropertySearch) {
                        logger.debug("{} Create missing Schema {} on database: {} / {}@{}", traceItem, XYZ_CONFIG_SCHEMA, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost());
                        stmt.execute(MaintenanceSQL.configSchemaAndSystemTablesSQL);
                    }

                    if (!idx_table && hasPropertySearch) {
                        /** Create Missing IDX_Maintenance Table */
                        stmt.execute(MaintenanceSQL.createIDXTableSQL);
                    }

                    if (!db_status_table) {
                        /** Create Missing DbStatus Table */
                        stmt.execute(MaintenanceSQL.createDbStatusTable);
                    }

                    if (!space_meta_table) {
                        /** Create Missing Space Meta Table */
                        stmt.execute(MaintenanceSQL.createSpaceMetaTable);
                    }

                    if (!tag_table) {
                        /** Create Missing Tag Table */
                        stmt.execute(MaintenanceSQL.createTagTable);
                    }

                    if (!subscription_table) {
                        /** Create Missing Tag Table */
                        stmt.execute(MaintenanceSQL.createSubscriptionTable);
                    }
                } catch (Exception e) {
                    logger.warn("{} Failed to create missing Schema(s) on database: {} / {}@{} '{}'", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost(), e);
                }
            }

            stmt.execute(MaintenanceSQL.generateSearchPathSQL(config.getDatabaseSettings().getSchema()));

            stmt = connection.createStatement();
            rs = stmt.executeQuery(MaintenanceSQL.generateEnsureExtVersionSQL(config.getDatabaseSettings().getSchema()));

            if (rs.next()) {
                /** If xyz_ext_version exists, use it to evaluate if the functions needs to get updated*/
                rs = stmt.executeQuery(MaintenanceSQL.generateEnsureCorrectVersionSQL(config.getDatabaseSettings().getSchema()));
                if (rs.next()) {
                    functionsUpToDate = (rs.getInt(1) == DatabaseMaintainer.XYZ_EXT_VERSION);
                }
            }

            if (!functionsUpToDate) {
                /** Need to apply the PSQL-script! */
                String content = readResource("/xyz_ext.sql");

                stmt = connection.createStatement();
                stmt.execute(MaintenanceSQL.generateSearchPathSQL( config.getDatabaseSettings().getSchema() ));
                stmt.execute(content);

                logger.info("{} Successfully created missing SQL-Functions on database: {} / {}@{}", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost());
            } else {
                logger.debug("{} All required SQL-Functions are already present on database: {} / {}@{}", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost());
            }
        } catch (Exception e) {
            logger.error("{} Failed to create missing SQL-Functions on database: {} / {}@{} '{}'", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost(), e);
        }

        /** Check if all required H3 related stuff is present */
        if(userHasCreatePermissions)
            this.setupH3(traceItem);
        else
            logger.warn("{} User permissions missing! Can not update/install H3 related functions on database': {} / {}@{}", traceItem,  config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost());
    }

    private synchronized void setupH3(TraceItem traceItem){
        if (true) // check h3 availability 1. test if version function exists, 2. test if version is outdated compared with H3CoreVersion
        {
            try (final Connection connection = dataSource.getConnection();
                 final Statement stmt = connection.createStatement();) {
                boolean needUpdate = false;

                ResultSet rs;

                if ((rs = stmt.executeQuery(
                        "select count(1)::integer from pg_catalog.pg_proc r inner join pg_catalog.pg_namespace l  on ( r.pronamespace = l.oid ) where 1 = 1 and l.nspname = 'h3' and r.proname = 'h3_version'"))
                        .next()) {
                    needUpdate = (0 == rs.getInt(1));
                }

                if (!needUpdate && (rs = stmt.executeQuery("select h3.h3_version()")).next()) {
                    needUpdate = (H3_CORE_VERSION > rs.getInt(1));
                }

                if (needUpdate) {
                    stmt.execute(readResource("/h3Core.sql"));
                    stmt.execute(MaintenanceSQL.generateSearchPathSQL( config.getDatabaseSettings().getSchema() ));
                    logger.debug("{} Successfully created H3 SQL-Functions on database: {} / {}@{} ", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost());
                }
            } catch (Exception e) {
                logger.error("{} Failed run h3 init on database: {} / {}@{} '{}'", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost(), e);
            }
        }
    }

    private synchronized void triggerIndexing(TraceItem traceItem, boolean autoIndexing){

        /** Trigger Auto-Indexing and or On-Demand Index Maintenance  */
        try (final Connection connection = dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(MaintenanceSQL.checkIDXStatus);

            int progress = 0;

            if (rs.next()) {
                /**
                 * ((progress  &  (1<<0)) == (1<<0) = statisticsInProgress
                 * ((progress  &  (1<<1)) == (1<<1) = analyseInProgress
                 * ((progress  &  (1<<2)) == (1<<2) = idxCreationInProgress
                 * ((progress  &  (1<<3)) == (1<<3) = idx_mode=16 (disable indexing completely)
                 */
                progress = rs.getInt(1);
            }

            if (progress == 0) {
                /** no process is running */
                /** Set Mode for statistic,analyze,index creation */
                int mode = autoIndexing == true ? 2 : 0;

                /** Maintain INDICES */
                stmt.execute(MaintenanceSQL.generateIDXSQL(config.getDatabaseSettings().getSchema(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getPassword(), config.getDatabaseSettings().getDb(),"localhost", config.getDatabaseSettings().getPort(), mode));
            }
        } catch (Exception e) {
            logger.error("{} Failed run indexing on database: {} / {}@{} '{}'", traceItem, config.getDatabaseSettings().getDb(), config.getDatabaseSettings().getUser(), config.getDatabaseSettings().getHost(), e);
        }
    }

    public SQLQuery maintainHistory(TraceItem traceItem, String schema, String table, long currentVersion, int maxVersionCount){
        long maxAllowedVersion = currentVersion - maxVersionCount;

        if(maxAllowedVersion <= 0)
            return null;

        if(config.getMaintenanceEndpoint() != null){
            try (CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()){

                HttpPost request = new HttpPost(config.getMaintenanceEndpoint()
                        +"/maintain/space/"+table+"/history?connectorId="+traceItem.getConnectorId()
                        +"&ecps="+config.getEcps()+"&maxVersionCount="+maxVersionCount+"&currentVersion="+currentVersion
                );

                HttpResponse response = client.execute(request);
                if(response.getStatusLine().getStatusCode() >= 400)
                    logger.warn("{} Could not maintain history! {}/{} {}", traceItem, schema, table, EntityUtils.toString(response.getEntity()));
            }catch (SocketTimeoutException | ConnectTimeoutException e){
                logger.info("{} Do not further wait for a response {}",traceItem,e);
            }catch (Exception e){
                logger.error("{} Could not maintain history! {}/{}", traceItem, schema, table);
            }
            return null;
        }else {
            SQLQuery q = new SQLQuery(SQLQueryBuilder.deleteOldHistoryEntries(schema, table + "_hst", maxAllowedVersion));
            q.append(new SQLQuery(SQLQueryBuilder.flagOutdatedHistoryEntries(schema, table + "_hst", maxAllowedVersion)));
            q.append(new SQLQuery(SQLQueryBuilder.deleteHistoryEntriesWithDeleteFlag(schema, table + "_hst")));
            return q;
        }
    }

    public void maintainSpace(TraceItem traceItem, String schema, String table){
        if(config.getMaintenanceEndpoint() != null){
            try (CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()){
                HttpPost request = new HttpPost(config.getMaintenanceEndpoint()
                        +"/maintain/space/"+table+"?connectorId="+traceItem.getConnectorId()
                        +"&ecps="+config.getEcps()+"&force=true"
                );

                HttpResponse response = client.execute(request);
                if(response.getStatusLine().getStatusCode() >= 400)
                    logger.warn("{} Could not maintain space!{}/{} {}", traceItem, schema, table, EntityUtils.toString(response.getEntity()));
            }catch (SocketTimeoutException | ConnectTimeoutException e){
                logger.info("{} Do not further wait for a response {}",traceItem,e);
            }catch (Exception e){
                logger.error("{} Could not maintain space!{}/{}", traceItem, schema, table);
            }
        }
    }

    public static String readResource(String resource) throws IOException {
        InputStream is = DatabaseHandler.class.getResourceAsStream(resource);
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
            return buffer.lines().collect(Collectors.joining("\n"));
        }
    }
}
