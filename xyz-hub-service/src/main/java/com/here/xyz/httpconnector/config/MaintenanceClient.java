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
import static com.here.xyz.psql.config.DatabaseSettings.PSQL_DB;
import static com.here.xyz.psql.config.DatabaseSettings.PSQL_HOST;
import static com.here.xyz.psql.config.DatabaseSettings.PSQL_PASSWORD;
import static com.here.xyz.psql.config.DatabaseSettings.PSQL_PORT;
import static com.here.xyz.psql.config.DatabaseSettings.PSQL_REPLICA_HOST;
import static com.here.xyz.psql.config.DatabaseSettings.PSQL_SCHEMA;
import static com.here.xyz.psql.config.DatabaseSettings.PSQL_USER;
import static com.here.xyz.psql.query.ModifySpace.IDX_STATUS_TABLE_FQN;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.Core;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.psql.DatabaseMaintainer;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.factory.MaintenanceSQL;
import com.here.xyz.responses.maintenance.ConnectorStatus;
import com.here.xyz.responses.maintenance.SpaceStatus;
import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import io.vertx.core.json.DecodeException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.NoPermissionException;
import javax.sql.DataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.StatementConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class MaintenanceClient {

    private static Map<String, MaintenanceInstance> dbInstanceMap = new HashMap<>();
    private static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()";

    private static final String[] extensionList = new String[]{"postgis","postgis_topology","tsm_system_rows","dblink","aws_s3"};
    private static final String[] localScripts = new String[]{"/xyz_ext.sql", "/h3Core.sql"};

    private static final Logger logger = LogManager.getLogger();

    public <T> T executeQuery(SQLQuery query, ResultSetHandler<T> handler, DataSource dataSource) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null, CService.configuration.DB_STATEMENT_TIMEOUT_IN_S));
            query.substitute();
            return run.query(query.text(), handler, query.parameters().toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("query time: {}ms", (end - start));
        }
    }

    public int executeQueryWithoutResults(SQLQuery query, DataSource dataSource) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            query.substitute();
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null, CService.configuration.DB_STATEMENT_TIMEOUT_IN_S));
            return run.execute(query.text(), query.parameters().toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("query time: {}ms", (end - start));
        }
    }

    public int executeUpdate(SQLQuery query, DataSource dataSource) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null,  CService.configuration.DB_STATEMENT_TIMEOUT_IN_S));
            query.substitute();
            final String queryText = query.text();
            final List<Object> queryParameters = query.parameters();

            return run.update(queryText, queryParameters.toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("query time: {}ms", (end - start));
        }
    }

    public ConnectorStatus getConnectorStatus(String connectorId, String ecps, String passphrase) throws SQLException, DecodeException{
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        SQLQuery query = new SQLQuery("select (select row_to_json( prop ) from ( select 'ConnectorStatus' as type, connector_id, initialized, " +
            "extensions, script_versions as \"scriptVersions\", maintenance_status as \"maintenanceStatus\") prop ) " +
            "from xyz_config.db_status where dh_schema = #{schema} AND connector_id = #{connectorId}")
            .withNamedParameter("schema", dbInstance.getDbSettings().getSchema())
            .withNamedParameter("connectorId", dbInstance.getConnectorId());

        /** If connector entry cant get found connector is not initialized */
        ConnectorStatus dbStatus = new ConnectorStatus().withInitialized(false);

        try {
            dbStatus = executeQuery(query, rs -> {
                if (rs.next()) {
                    try {
                        return XyzSerializable.deserialize(rs.getString(1), ConnectorStatus.class);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException("Cant serialize DatabaseStatus");
                    }
                }
                return null;
            }, dbInstance.getSource());
        }catch (SQLException e){
            if(e.getSQLState() != null && e.getSQLState().equals("42P01")){
                /** Table Not Exists => DB not initialized */
                new ConnectorStatus().withInitialized(false);
            }else if(e.getSQLState() == null && e.getMessage().equalsIgnoreCase(""))
                ;
            else
                throw e;
        }
        return dbStatus;
    }

    public void executeLocalScripts(ComboPooledDataSource source, String[] localPaths) throws IOException, SQLException {
        for (String path : localPaths) {
            String content = DatabaseMaintainer.readResource(path);
            /** Create required Extensions */
            logger.info("{}: Apply Script \"{}\" ..",path);
            executeQueryWithoutResults(new SQLQuery(content), source);
        }
        //Clear open Connections to reset searchpath
        source.resetPoolManager();
    }

    public void initializeOrUpdateDatabase(String connectorId, String ecps, String passphrase) throws SQLException, NoPermissionException, IOException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();
        DataSource source = dbInstance.getSource();

        SQLQuery hasPermissionsQuery = new SQLQuery("select has_database_privilege(#{user}, #{db}, 'CREATE')")
            .withNamedParameter("user", dbSettings.getUser())
            .withNamedParameter("db", dbSettings.getDb());

        SQLQuery installExtensionsQuery = new SQLQuery(MaintenanceSQL.generateMandatoryExtensionSQL());

        SQLQuery setSearchpath = new SQLQuery(MaintenanceSQL.generateSearchPathSQL(dbSettings.getSchema()));
        //Create XYZ_CONFIG_SCHEMA and required system tables
        SQLQuery addInitializationEntry = new SQLQuery("INSERT INTO xyz_config.db_status as a (dh_schema, connector_id, initialized, extensions, script_versions, maintenance_status) " +
            "VALUES (#{schema}, #{connectorId}, #{initialized}, #{extensions}, #{scriptVersions}::jsonb, #{maintenanceStatus}::jsonb)"
            + "ON CONFLICT (dh_schema,connector_id) DO "
            + "UPDATE SET extensions = #{extensions},"
            + "    		  script_versions = #{scriptVersions}::jsonb,"
            + "    		  initialized = #{initialized}"
            + "		WHERE a.dh_schema = #{schema} AND a.connector_id = #{connectorId}")
            .withNamedParameter("schema", dbSettings.getSchema())
            .withNamedParameter("connectorId", dbInstance.getConnectorId())
            .withNamedParameter("initialized", true)
            .withNamedParameter("extensions", extensionList)
            .withNamedParameter("scriptVersions", "{\"ext\": " + DatabaseMaintainer.XYZ_EXT_VERSION + ", \"h3\": " + DatabaseMaintainer.H3_CORE_VERSION + "}")
            .withNamedParameter("maintenanceStatus", null);

        boolean hasPermissions = executeQuery(hasPermissionsQuery, rs -> {
            if(rs.next())
                return rs.getBoolean(1);
            return false;
        }, source);

        if(hasPermissions){
            logger.info("{}: Create required Extensions..", connectorId);
            executeQueryWithoutResults(installExtensionsQuery, source);
            /** Create required Schemas and Tables */

            logger.info("{}: Create required Main-Schema..", connectorId);
            executeQueryWithoutResults(new SQLQuery("CREATE SCHEMA IF NOT EXISTS \"" + dbSettings.getSchema() + "\";"), source);

            logger.info("{}: Create required Config-Schema and System-Tables..", connectorId);
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.configSchemaAndSystemTablesSQL), source);

            logger.info("{}: Create required IDX-Table..", connectorId);
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.createIDXTable), source);

            logger.info("{}: Add initial IDX Entry", connectorId);
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.createIDXInitEntry), source);

            logger.info("{}: Create required DB-Status-Table..", connectorId);
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.createDbStatusTable), source);

            logger.info("{}: Create required Space-Meta-Table..", connectorId);
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.createSpaceMetaTable), source);

            logger.info("{}: Create required Tag-Table..", connectorId);
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.createTagTable), source);

            logger.info("{}: Create required Subscription-Table..", connectorId);
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.createSubscriptionTable), source);

            /** set searchPath */
            executeQueryWithoutResults(setSearchpath, source);
            /** Install extensions */
            executeLocalScripts((ComboPooledDataSource)source, localScripts);

            /** Mark db initialization as finished in DBStatus table*/
            logger.info("{}: Mark db as initialized in db-status table..", connectorId);
            executeQueryWithoutResults(addInitializationEntry, source);
        }else{
            throw new NoPermissionException("");
        }
    }

    public void maintainIndices(String connectorId, String ecps, String passphrase, boolean autoIndexing, boolean force) throws SQLException,DecodeException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();
        DataSource source = dbInstance.getSource();
        String[] dbUser = new String[]{"wikvaya", dbSettings.getUser()};
        String maintenanceJobId = ""+Core.currentTimeMillis();
        int mode = autoIndexing == true ? 2 : 0;

        Integer status = executeQuery(new SQLQuery(MaintenanceSQL.checkIDXStatus), rs -> {
            if (rs.next()) {
                //((progress  &  (1<<3)) == (1<<3) = idx_mode=16 (disable indexing completely)
                return rs.getInt(1);
            }
            return -1;
        }, source);

        if(status == 16 ) {
            logger.info("{}: Indexing is disabled database wide! ", connectorId);
            return;
        }

        SQLQuery updateDBStatus=(force == false ?
                      new SQLQuery("UPDATE xyz_config.db_status SET maintenance_status=" +
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
            .withNamedParameter("connectorId", dbInstance.connectorId);

        executeUpdate(updateDBStatus, source);
        try {
            SQLQuery triggerIndexing = new SQLQuery("SELECT xyz_create_idxs(#{schema}, #{limit}, #{offset}, #{mode}, #{user})")
                .withNamedParameter("schema", dbSettings.getSchema())
                .withNamedParameter("limit", 100)
                .withNamedParameter("offset", 0)
                .withNamedParameter("mode", mode)
                .withNamedParameter("user", dbUser);

            logger.info("{}: Start Indexing..", connectorId);
            executeQueryWithoutResults(triggerIndexing, source);
        }
        finally {
            updateDBStatus = new SQLQuery("UPDATE xyz_config.db_status SET maintenance_status =  "+
                "jsonb_set( jsonb_set(maintenance_status,'{AUTO_INDEXING,maintainedAt}'::text[], #{maintenanceJobId}::jsonb),'{AUTO_INDEXING,maintenanceRunning}'," +
                "   COALESCE((select jsonb_agg(jsonb_array_elements) from jsonb_array_elements(maintenance_status->'AUTO_INDEXING'->'maintenanceRunning')" +
                "           where jsonb_array_elements != #{maintenanceJobId}::jsonb), '[]'::jsonb)" +
                "    )" +
                "    WHERE dh_schema = #{schema} AND connector_id = #{connectorId}")
                .withNamedParameter("maintenanceJobId", maintenanceJobId)
                .withNamedParameter("schema", dbInstance.getDbSettings().getSchema())
                .withNamedParameter("connectorId", dbInstance.connectorId);
            logger.info("{}: Mark Indexing as finished", connectorId);
            executeUpdate(updateDBStatus, source);
        }
    }

    public SpaceStatus getMaintenanceStatusOfSpace(String connectorId, String ecps, String passphrase, String spaceId) throws SQLException,DecodeException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();

        SQLQuery statusQuery = new SQLQuery("select (" +
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
            .withNamedParameter("spaceId", spaceId);
        logger.info("{}: Get maintenanceStatus of space '{}'..", connectorId, spaceId);

        return executeQuery(statusQuery, rs -> {
            if (rs.next()) {
                try {
                    return XyzSerializable.deserialize(rs.getString(1), SpaceStatus.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("{}: Cant serialize SpaceStatus");
                }
            }
            return null;
        }, dbInstance.getSource());
    }

    public void maintainSpace(String connectorId, String ecps, String passphrase, String spaceId) throws SQLException,DecodeException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();
        DataSource source = dbInstance.getSource();

        SQLQuery updateIDXEntry = new SQLQuery("UPDATE " + IDX_STATUS_TABLE_FQN
            + " SET idx_creation_finished = null "
            + " WHERE schem = #{schema} AND spaceid = #{spaceId}")
            .withNamedParameter("schema", dbSettings.getSchema())
            .withNamedParameter("spaceId", spaceId);
        logger.info("{}: Set idx_creation_finished=NULL {}", connectorId, spaceId);
        executeQueryWithoutResults(updateIDXEntry, source);

        SQLQuery maintainSpace = new SQLQuery("select xyz_maintain_idxs_for_space(#{schema}, #{spaceId})")
            .withNamedParameter("schema", dbSettings.getSchema())
            .withNamedParameter("spaceId", spaceId);
        logger.info("{}: Start maintaining space '{}'..", connectorId, spaceId);
        executeQueryWithoutResults(maintainSpace, source);
    }

    public void purgeOldVersions(String connectorId, String ecps, String passphrase, String spaceId, Integer versionsToKeep, Long minTagVersion) throws SQLException,DecodeException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();
        DataSource source = dbInstance.getSource();

        SQLQuery q =  new SQLQuery("SELECT xyz_advanced_delete_changesets(#{schema}, #{table}, #{partitionSize}, #{versionsToKeep}, #{minTagVersion}, #{pw})")
            .withNamedParameter("schema",dbSettings.getSchema())
            .withNamedParameter("table", spaceId)
            .withNamedParameter("partitionSize", PARTITION_SIZE)
            .withNamedParameter("versionsToKeep", versionsToKeep)
            .withNamedParameter("minTagVersion", minTagVersion)
            .withNamedParameter("pw", dbSettings.getPassword());

        executeQueryWithoutResults(q, source);
    }

    public synchronized MaintenanceInstance getClient(String connectorId, String ecps, String passphrase) throws DecodeException{
        DatabaseSettings dbSettings;
        dbSettings = readDBSettingsFromECPS(ecps,passphrase);

        MaintenanceInstance maintenanceInstance = new MaintenanceInstance(connectorId, dbSettings);

        if(dbInstanceMap.get(connectorId) != null){
            /** Check if db-params has changed */
            if(!dbInstanceMap.get(connectorId).getConfigValuesAsString().equalsIgnoreCase(maintenanceInstance.getConfigValuesAsString())) {
                removeDbInstanceFromMap(connectorId);
            }else{
                logger.debug("{}: Config already loaded -> load dbInstance from Pool. DbInstanceMap size:{}", connectorId, dbInstanceMap.size());
                return dbInstanceMap.get(connectorId);
            }
        }

        /** Init dataSource, readDataSource ..*/
        logger.info("{}: Config is missing -> add new dbInstance to Pool. DbInstanceMap size:{}", connectorId, dbInstanceMap.size());
        final ComboPooledDataSource source = getComboPooledDataSource(dbSettings, connectorId+"["+(ecps.length() < 10 ? ecps : ecps.substring(0,9))+"]", false);

        Map<String, String> m = new HashMap<>();
        m.put(C3P0EXT_CONFIG_SCHEMA, dbSettings.getSchema());
        source.setExtensions(m);
        maintenanceInstance.setSource(source);
        dbInstanceMap.put(connectorId, maintenanceInstance);
        return maintenanceInstance;
    }

    private void removeDbInstanceFromMap(String connectorId){
        synchronized (dbInstanceMap) {
            try {
                ((PooledDataSource) (dbInstanceMap.get(connectorId).getSource())).close();
            } catch (SQLException e) {
                logger.warn("{}: Error while closing connections: {}", connectorId, e);
            }
            dbInstanceMap.remove(connectorId);
        }
    }

    private ComboPooledDataSource getComboPooledDataSource(DatabaseSettings dbSettings, String applicationName, boolean useReplica) {
        final ComboPooledDataSource cpds = new ComboPooledDataSource();

        cpds.setJdbcUrl("jdbc:postgresql://" + (useReplica ? dbSettings.getReplicaHost() : dbSettings.getHost()) + ":"
            + dbSettings.getPort() + "/" + dbSettings.getDb() + "?ApplicationName=" + applicationName + "&tcpKeepAlive=true");

        cpds.setUser(dbSettings.getUser());
        cpds.setPassword(dbSettings.getPassword());

        cpds.setInitialPoolSize( CService.configuration.DB_INITIAL_POOL_SIZE);
        cpds.setMinPoolSize( CService.configuration.DB_MIN_POOL_SIZE);
        cpds.setMaxPoolSize( CService.configuration.DB_MAX_POOL_SIZE);

        cpds.setAcquireRetryAttempts( CService.configuration.DB_ACQUIRE_RETRY_ATTEMPTS);
        cpds.setAcquireIncrement( CService.configuration.DB_ACQUIRE_INCREMENT);

        cpds.setCheckoutTimeout( CService.configuration.DB_CHECKOUT_TIMEOUT * 1000 );
        cpds.setTestConnectionOnCheckout( CService.configuration.DB_TEST_CONNECTION_ON_CHECKOUT);

        cpds.setConnectionCustomizerClassName(ConnectionCustomizer.class.getName());
        return cpds;
    }

    public static class ConnectionCustomizer extends AbstractConnectionCustomizer { // handle initialization per db connection
        private String getSchema(String parentDataSourceIdentityToken) {
            return (String) extensionsForToken(parentDataSourceIdentityToken).get(C3P0EXT_CONFIG_SCHEMA);
        }

        public void onAcquire(Connection c, String pdsIdt) {
            String schema = getSchema(pdsIdt);  // config.schema();
            QueryRunner runner = new QueryRunner();
            try {
                runner.execute(c, "SET enable_seqscan = off;");
                runner.execute(c, "SET statement_timeout = " + (  CService.configuration.DB_STATEMENT_TIMEOUT_IN_S * 1000) + " ;");
                runner.execute(c, "SET search_path=" + schema + ",h3,public,topology;");
            } catch (SQLException e) {
                logger.error("Failed to initialize connection " + c + " [" + pdsIdt + "] : {}", e);
            }
        }
    }

    public DatabaseSettings readDBSettingsFromECPS(String ecps, String passphrase) throws DecodeException{
        Map<String, Object> decodedEcps = new HashMap<>();
        try {
            if(ecps != null)
                ecps = ecps.replaceAll(" ","+");
            decodedEcps = PSQLConfig.decryptECPS(ecps, passphrase);
        }catch (Exception e){
            logger.error("ECPS Decryption has failed");
            throw new DecodeException("ECPS Decryption has failed");
        }

        DatabaseSettings databaseSettings = new DatabaseSettings();

        for (String key : decodedEcps.keySet()) {
            switch (key) {
                case DatabaseSettings.PSQL_DB:
                    databaseSettings.setDb((String) decodedEcps.get(PSQL_DB));
                    break;
                case PSQL_HOST:
                    databaseSettings.setHost((String) decodedEcps.get(PSQL_HOST));
                    break;
                case PSQL_PASSWORD:
                    databaseSettings.setPassword((String) decodedEcps.get(PSQL_PASSWORD));
                    break;
                case PSQL_PORT:
                    databaseSettings.setPort(Integer.parseInt((String) decodedEcps.get(PSQL_PORT)));
                    break;
                case PSQL_REPLICA_HOST:
                    databaseSettings.setReplicaHost((String) decodedEcps.get(PSQL_REPLICA_HOST));
                    break;
                case PSQL_SCHEMA:
                    databaseSettings.setSchema((String) decodedEcps.get(PSQL_SCHEMA));
                    break;
                case PSQL_USER:
                    databaseSettings.setUser((String) decodedEcps.get(PSQL_USER));
                    break;
            }
        }
        return databaseSettings;
    }

    public class MaintenanceInstance {
        private String connectorId;
        private final DatabaseSettings dbSettings;
        private ComboPooledDataSource source;

        public MaintenanceInstance(String connectorId, DatabaseSettings dbSettings){
            this.connectorId = connectorId;
            this.dbSettings = dbSettings;
        }

        public ComboPooledDataSource getSource(){
            return source;
        }

        public void setSource(ComboPooledDataSource source){
            this.source = source;
        }

        public String getConnectorId(){
            return this.connectorId;
        }

        public DatabaseSettings getDbSettings(){ return this.dbSettings; }

        public String getConfigValuesAsString() {
            return this.connectorId+this.dbSettings.getConfigValuesAsString();
        }
    }
}
