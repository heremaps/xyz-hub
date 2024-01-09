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
import com.here.xyz.psql.config.ConnectorParameters;
import com.here.xyz.psql.factory.MaintenanceSQL;
import com.here.xyz.util.db.DatabaseSettings;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatabaseMaintainer {
    private static final Logger logger = LogManager.getLogger();

    /** Is used to check against xyz_ext_version() */
    public static final int XYZ_EXT_VERSION = 184;

    public static final int H3_CORE_VERSION = 108;

    private DataSourceProvider dataSourceProvider;
    private DatabaseSettings dbSettings;
    private final String maintenanceEndpoint;
    private final ConnectorParameters connectorParameters;

    private final RequestConfig requestConfig = RequestConfig.custom()
            .setConnectTimeout(1 * 1000 )
            .setConnectionRequestTimeout(1 * 1000)
            .setSocketTimeout(1 * 1000)
            .build();

    public DatabaseMaintainer(DataSourceProvider dataSourceProvider, DatabaseSettings dbSettings, ConnectorParameters connectorParameters, String maintenanceEndpoint) {
        this.dataSourceProvider = dataSourceProvider;
        this.dbSettings = dbSettings;
        this.connectorParameters = connectorParameters;
        this.maintenanceEndpoint = maintenanceEndpoint;
    }

    public synchronized void run(TraceItem traceItem) {
        final boolean propertySearch = connectorParameters.isPropertySearch();
        final boolean autoIndexing = connectorParameters.isAutoIndexing();

        if (maintenanceEndpoint != null) {
            try (CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()) {
                HttpPost request = new HttpPost(maintenanceEndpoint + "/maintain/connectors/" + traceItem.getConnectorId()
                    + "/indices");

                HttpResponse response = client.execute(request);
                if (response.getStatusLine().getStatusCode() == 405){
                    logger.warn("{} Database not initialized!",traceItem);
                    request = new HttpPost(maintenanceEndpoint
                            +"/maintain/connectors/" + traceItem.getConnectorId() + "/initialization");
                    response = client.execute(request);
                    if (response.getStatusLine().getStatusCode() >= 400)
                        logger.warn("{} Could not initialize Database! {}",traceItem, EntityUtils.toString(response.getEntity()));
                }
                else if (response.getStatusLine().getStatusCode() == 409)
                    logger.info("{} Maintenance already running! {}", traceItem, EntityUtils.toString(response.getEntity()));
                else if (response.getStatusLine().getStatusCode() >= 400)
                    logger.warn("{} Could not maintain Database! {}", traceItem, EntityUtils.toString(response.getEntity()));
            }
            catch (SocketTimeoutException | ConnectTimeoutException e){
                logger.info("{} Do not further wait for a response {}",traceItem,e);
            }
            catch (Exception e){
                logger.error("{} Could not maintain Database! {}",traceItem,e);
            }
        }
        else {
            //Check if all required extensions, schemas, tables and functions are present
            this.initialDBSetup(traceItem, autoIndexing, propertySearch);

            if (propertySearch)
                //Trigger missing Index Maintenance (On-Demand & Auto-Indexing)
                this.triggerIndexing(traceItem, autoIndexing);
        }
    }

    private synchronized void initialDBSetup(TraceItem traceItem, boolean autoIndexing, boolean propertySearch) {
        boolean userHasCreatePermissions = false;

        try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {
            boolean functionsUpToDate = false;

            //Check if database is prepared to work with PSQL Connector. Therefore, it's needed to check Extensions, Schemas, Tables and Functions.
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(MaintenanceSQL.generateCheckExtensionsSQL(propertySearch, dbSettings.getUser(), dbSettings.getDb()));

            if (rs.next()) {
                userHasCreatePermissions = rs.getBoolean("has_create_permissions");
                if (!rs.getBoolean("all_ext_av")) {
                    //Create Missing IDX_Maintenance Table
                    if (userHasCreatePermissions) {
                        stmt.execute(MaintenanceSQL.generateMandatoryExtensionSQL());
                    } else {
                        logger.error("{} User permissions missing! Not able to create missing Extensions on database: {}@{} / {}. Installed Extension are: {}",
                                traceItem, dbSettings.getUser(), dbSettings.getDb(), dbSettings.getDb(), rs.getString("ext_av"));
                        /*
                        Cannot proceed without extensions!
                        postgis,postgis_topology -> provides all GIS functions which are essential!
                        tsm_system_rows -> Is used for generating statistics
                        dblink -> Is used for Auto+On-Demand Indexing
                        Without
                         */
                        return;
                    }
                }
            }

            stmt = connection.createStatement();
            //Find Missing Schemas and check if IDX_Maintenance Table is available
            rs = stmt.executeQuery(MaintenanceSQL.generateCheckSchemasAndIdxTableSQL(dbSettings.getSchema()));

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

                    //Create Missing Schemas
                    if (!mainSchema) {
                        logger.debug("{} Create missing Schema {} on database: {} / {}@{}", traceItem, dbSettings.getSchema(), dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost());
                        stmt.execute(MaintenanceSQL.generateMainSchemaSQL(dbSettings.getSchema()));
                    }

                    if (!configSchema && propertySearch) {
                        logger.debug("{} Create missing Schema {} on database: {} / {}@{}", traceItem, XYZ_CONFIG_SCHEMA, dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost());
                        stmt.execute(MaintenanceSQL.configSchemaAndSystemTablesSQL);
                    }

                    if (!idx_table && propertySearch)
                        //Create Missing IDX_Maintenance Table
                        stmt.execute(MaintenanceSQL.createIDXTableSQL);

                    if (!db_status_table)
                        //Create Missing DbStatus Table
                        stmt.execute(MaintenanceSQL.createDbStatusTable);

                    if (!space_meta_table)
                        //Create Missing Space Meta Table
                        stmt.execute(MaintenanceSQL.createSpaceMetaTable);

                    if (!tag_table)
                        //Create Missing Tag Table
                        stmt.execute(MaintenanceSQL.createTagTable);

                    if (!subscription_table)
                        //Create Missing Tag Table
                        stmt.execute(MaintenanceSQL.createSubscriptionTable);
                }
                catch (Exception e) {
                    logger.warn("{} Failed to create missing Schema(s) on database: {} / {}@{} '{}'", traceItem, dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost(), e);
                }
            }

            stmt.execute(MaintenanceSQL.generateSearchPathSQL(dbSettings.getSchema()));

            stmt = connection.createStatement();
            rs = stmt.executeQuery(MaintenanceSQL.generateEnsureExtVersionSQL(dbSettings.getSchema()));

            if (rs.next()) {
                //If xyz_ext_version exists, use it to evaluate if the functions needs to get update
                rs = stmt.executeQuery(MaintenanceSQL.generateEnsureCorrectVersionSQL(dbSettings.getSchema()));
                if (rs.next())
                    functionsUpToDate = (rs.getInt(1) == DatabaseMaintainer.XYZ_EXT_VERSION);
            }

            if (!functionsUpToDate) {
                //Need to apply the PSQL-script!
                String content = readResource("/xyz_ext.sql");

                stmt = connection.createStatement();
                stmt.execute(MaintenanceSQL.generateSearchPathSQL(dbSettings.getSchema()));
                stmt.execute(content);

                logger.info("{} Successfully created missing SQL-Functions on database: {} / {}@{}", traceItem, dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost());
            }
            else
                logger.debug("{} All required SQL-Functions are already present on database: {} / {}@{}", traceItem, dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost());
        }
        catch (Exception e) {
            logger.error("{} Failed to create missing SQL-Functions on database: {} / {}@{} '{}'", traceItem, dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost(), e);
        }

        //Check if all required H3 related stuff is present
        if (userHasCreatePermissions)
            this.setupH3(traceItem);
        else
            logger.warn("{} User permissions missing! Can not update/install H3 related functions on database': {} / {}@{}", traceItem,  dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost());
    }

    private synchronized void setupH3(TraceItem traceItem){
        if (true) { // check h3 availability 1. test if version function exists, 2. test if version is outdated compared with H3CoreVersion
            try (final Connection connection = dataSourceProvider.getWriter().getConnection();
                 final Statement stmt = connection.createStatement();) {
                boolean needUpdate = false;

                ResultSet rs;

                if ((rs = stmt.executeQuery(
                        "select count(1)::integer from pg_catalog.pg_proc r inner join pg_catalog.pg_namespace l  on ( r.pronamespace = l.oid ) where 1 = 1 and l.nspname = 'h3' and r.proname = 'h3_version'"))
                        .next())
                    needUpdate = (0 == rs.getInt(1));

                if (!needUpdate && (rs = stmt.executeQuery("select h3.h3_version()")).next())
                    needUpdate = (H3_CORE_VERSION > rs.getInt(1));

                if (needUpdate) {
                    stmt.execute(readResource("/h3Core.sql"));
                    stmt.execute(MaintenanceSQL.generateSearchPathSQL(dbSettings.getSchema()));
                    logger.debug("{} Successfully created H3 SQL-Functions on database: {} / {}@{} ", traceItem, dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost());
                }
            }
            catch (Exception e) {
                logger.error("{} Failed run h3 init on database: {} / {}@{} '{}'", traceItem, dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost(), e);
            }
        }
    }

    private synchronized void triggerIndexing(TraceItem traceItem, boolean autoIndexing){

        /** Trigger Auto-Indexing and or On-Demand Index Maintenance  */
        try (final Connection connection = dataSourceProvider.getWriter().getConnection()) {
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
                stmt.execute(MaintenanceSQL.generateIDXSQL(dbSettings.getSchema(), dbSettings.getUser(), dbSettings.getPassword(), dbSettings.getDb(),"localhost", dbSettings.getPort(), mode));
            }
        }
        catch (Exception e) {
            logger.error("{} Failed run indexing on database: {} / {}@{} '{}'", traceItem, dbSettings.getDb(), dbSettings.getUser(), dbSettings.getHost(), e);
        }
    }

    public void maintainSpace(String streamId, String schema, String spaceId) {
        if (maintenanceEndpoint != null) {
            try (CloseableHttpClient client = HttpClients.custom().setDefaultRequestConfig(requestConfig).build()) {
                HttpPost request = new HttpPost(maintenanceEndpoint + "/maintain/spaces/" + spaceId + "?force=true");
                HttpResponse response = client.execute(request);
                if (response.getStatusLine().getStatusCode() >= 400)
                    logger.warn("{} Could not maintain space!{}/{} {}", streamId, schema, spaceId, EntityUtils.toString(response.getEntity()));
            }
            catch (SocketTimeoutException | ConnectTimeoutException e){
                logger.info("{} Do not further wait for a response {}",streamId,e);
            }
            catch (Exception e){
                logger.error("{} Could not maintain space!{}/{}", streamId, schema, spaceId);
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
