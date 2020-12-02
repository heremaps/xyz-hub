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

package com.here.xyz.psql;

import com.here.xyz.events.Event;
import com.here.xyz.psql.factory.MaintenanceSQL;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.stream.Collectors;

public class DatabaseMaintainer {
    private static final Logger logger = LogManager.getLogger();

    /** Is used to check against xyz_ext_version() */
    private static final int XYZ_EXT_VERSION = 133;
    /** Can get configured dynamically with storageParam onDemandIdxLimit */
    protected final static int ON_DEMAND_IDX_DEFAULT_LIM = 4;

    private DataSource dataSource;
    private PSQLConfig config;

    public DatabaseMaintainer(DataSource dataSource, PSQLConfig config){
        this.dataSource = dataSource;
        this.config = config;
    }

    public synchronized void run(Event event, String streamId) {
        final boolean hasPropertySearch = config.isPropertySearchActivated();
        final boolean autoIndexing = config.isAutoIndexingActivated();

        /** Check if all required extensions, schemas, tables and functions are present  */
        this.initialDBSetup(streamId, autoIndexing, hasPropertySearch);

        if (hasPropertySearch) {
            /** Trigger missing Index Maintenance (On-Demand & Auto-Indexing) */
            this.triggerIndexing(streamId, autoIndexing);
        }
    }

    private synchronized void initialDBSetup(String streamId, boolean autoIndexing, boolean hasPropertySearch){
        boolean userHasCreatePermissions = false;

        try (final Connection connection = dataSource.getConnection()) {
            boolean functionsUpToDate = false;

            /** Check if database is prepared to work with PSQL Connector. Therefore its needed to check Extensions, Schemas, Tables and Functions.*/
            Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(MaintenanceSQL.generateCheckExtensionsSQL(hasPropertySearch, config.user(), config.database()));

            if (rs.next()) {
                userHasCreatePermissions = rs.getBoolean("has_create_permissions");
                if (!rs.getBoolean("all_ext_av")) {
                    /** Create Missing IDX_Maintenance Table */
                    if (userHasCreatePermissions) {
                        stmt.execute(MaintenanceSQL.generateMandatoryExtensionSQL(hasPropertySearch));
                    } else {
                        logger.error("{} - User permissions missing! Not able to create missing Extensions on database: {}@{} / {}. Installed Extension are: {}",
                                streamId, config.user(), config.database(), config.host(), rs.getString("ext_av"));
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
            rs = stmt.executeQuery(MaintenanceSQL.generateCheckSchemasAndIdxTableSQL(config.schema()));

            if (rs.next()) {
                final boolean mainSchema = rs.getBoolean("main_schema");
                boolean configSchema = rs.getBoolean("config_schema");
                final boolean idx_table = rs.getBoolean("idx_table");

                try {
                    /** Create Missing Schemas */
                    if (!mainSchema) {
                        logger.debug("{} - Create missing Schema {} on database: {} / {}@{}", streamId, config.schema(), config.database(), config.user(), config.host());
                        stmt.execute(MaintenanceSQL.generateMainSchemaSQL(config.schema()));
                    }

                    if (!configSchema && hasPropertySearch) {
                        logger.debug("{} - Create missing Schema {} on database: {} / {}@{}", streamId, MaintenanceSQL.XYZ_CONFIG_SCHEMA, config.database(), config.user(), config.host());
                        stmt.execute(MaintenanceSQL.configSchemaAndSystemTablesSQL);
                    }

                    if (!idx_table && hasPropertySearch) {
                        /** Create Missing IDX_Maintenance Table */
                        stmt.execute(MaintenanceSQL.createIDXTableSQL);
                    }
                } catch (Exception e) {
                    logger.warn("{} - Failed to create missing Schema(s) on database: {} / {}@{} '{}'", streamId, config.database(), config.user(), config.host(), e);
                }
            }

            stmt.execute(MaintenanceSQL.generateSearchPathSQL(config.schema()));

            stmt = connection.createStatement();
            rs = stmt.executeQuery(MaintenanceSQL.generateEnsureExtVersionSQL(config.schema()));

            if (rs.next()) {
                /** If xyz_ext_version exists, use it to evaluate if the functions needs to get updated*/
                rs = stmt.executeQuery(MaintenanceSQL.generateEnsureCorrectVersionSQL(config.schema()));
                if (rs.next()) {
                    functionsUpToDate = (rs.getInt(1) == DatabaseMaintainer.XYZ_EXT_VERSION);
                }
            }

            if (!functionsUpToDate) {
                /** Need to apply the PSQL-script! */
                String content = readResource("/xyz_ext.sql");

                stmt = connection.createStatement();
                stmt.execute(MaintenanceSQL.generateSearchPathSQL( config.schema() ));
                stmt.execute(content);

                logger.debug("{} - Successfully created missing SQL-Functions on database: {} / {}@{}", streamId, config.database(), config.user(), config.host());
            } else {
                logger.debug("{} - All required SQL-Functions are already present on database: {} / {}@{}", streamId, config.database(), config.user(), config.host());
            }
        } catch (Exception e) {
            logger.error("{} - Failed to create missing SQL-Functions on database: {} / {}@{} '{}'", streamId, config.database(), config.user(), config.host(), e);
        }

        /** Check if all required H3 related stuff is present */
        if(userHasCreatePermissions)
            this.setupH3( streamId);
        else
            logger.warn("{} - User permissions missing! Can not update/install H3 related functions on database': {} / {}@{}", streamId,  config.database(), config.user(), config.host());
    }

    private synchronized void setupH3(String streamId){
        if (true) // check h3 availability 1. test if version function exists, 2. test if version is outdated compared with H3CoreVersion
        {
            try (final Connection connection = dataSource.getConnection();
                 final Statement stmt = connection.createStatement();) {
                final int H3CoreVersion = 106;
                boolean needUpdate = false;

                ResultSet rs;

                if ((rs = stmt.executeQuery(
                        "select count(1)::integer from pg_catalog.pg_proc r inner join pg_catalog.pg_namespace l  on ( r.pronamespace = l.oid ) where 1 = 1 and l.nspname = 'h3' and r.proname = 'h3_version'"))
                        .next()) {
                    needUpdate = (0 == rs.getInt(1));
                }

                if (!needUpdate && (rs = stmt.executeQuery("select h3.h3_version()")).next()) {
                    needUpdate = (H3CoreVersion > rs.getInt(1));
                }

                if (needUpdate) {
                    stmt.execute(readResource("/h3Core.sql"));
                    stmt.execute(MaintenanceSQL.generateSearchPathSQL( config.schema() ));
                    logger.debug("{} - Successfully created H3 SQL-Functions on database: {} / {}@{} ", streamId, config.database(), config.user(), config.host());
                }
            } catch (Exception e) {
                logger.error("{} - Failed run h3 init on database: {} / {}@{} '{}'", streamId, config.database(), config.user(), config.host(), e);
            }
        }
    }

    private synchronized void triggerIndexing(String streamId, boolean autoIndexing){

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
                stmt.execute(MaintenanceSQL.generateIDXSQL(config.schema(), config.user(), config.password(), config.database(),"localhost", config.port(), mode));
            }
        } catch (Exception e) {
            logger.error("{} - Failed run indexing on database: {} / {}@{} '{}'", streamId, config.database(), config.user(), config.host(), e);
        }
    }

    private String readResource(String resource) throws IOException {
        InputStream is = DatabaseHandler.class.getResourceAsStream(resource);
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(is))) {
            return buffer.lines().collect(Collectors.joining("\n"));
        }
    }
}
