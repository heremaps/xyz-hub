/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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
package com.here.xyz.hub.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.XyzSerializable;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.PsqlHttpVerticle;
import com.here.xyz.psql.DatabaseMaintainer;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.SQLQueryBuilder;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.config.PSQLConfig;
import com.here.xyz.psql.factory.MaintenanceSQL;
import com.here.xyz.responses.maintenance.ConnectorStatus;
import com.here.xyz.responses.maintenance.SpaceStatus;
import com.mchange.v2.c3p0.AbstractConnectionCustomizer;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.mchange.v2.c3p0.PooledDataSource;
import io.vertx.core.json.DecodeException;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.apache.commons.dbutils.StatementConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.naming.NoPermissionException;
import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.here.xyz.psql.config.DatabaseSettings.*;

public class MaintenanceClient {

    private static Map<String, MaintenanceInstance> dbInstanceMap = new HashMap<>();
    private static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()";

    private static final int STATEMENT_TIMEOUT = Integer.parseInt(PsqlHttpVerticle.getEnvMap().get("DB_STATEMENT_TIMEOUT_IN_S"));
    private static final String[] extensionList = new String[]{"postgis","postgis_topology","tsm_system_rows","dblink"};
    private static final String[] localScripts = new String[]{"/xyz_ext.sql", "/h3Core.sql"};

    private static final Logger logger = LogManager.getLogger();

    public <T> T executeQuery(SQLQuery query, ResultSetHandler<T> handler, DataSource dataSource) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null, STATEMENT_TIMEOUT));
            return run.query(query.text(), handler, query.parameters().toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("query time: {}ms", (end - start));
        }
    }

    public int executeQueryWithoutResults(SQLQuery query, DataSource dataSource) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null, STATEMENT_TIMEOUT));
            return run.execute(query.text(), query.parameters().toArray());
        } finally {
            final long end = System.currentTimeMillis();
            logger.info("query time: {}ms", (end - start));
        }
    }

    public int executeUpdate(SQLQuery query, DataSource dataSource) throws SQLException {
        final long start = System.currentTimeMillis();
        try {
            final QueryRunner run = new QueryRunner(dataSource, new StatementConfiguration(null,null,null,null, STATEMENT_TIMEOUT));
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
        SQLQuery query = new SQLQuery(MaintenanceSQL.getConnectorStatus, dbInstance.getDbSettings().getSchema(), dbInstance.getConnectorId());

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

    public void executeLocalScripts(DataSource source, String[] localPaths) throws IOException, SQLException {
        for (String path : localPaths) {
            String content = DatabaseMaintainer.readResource(path);
            /** Create required Extensions */
            logger.info("Apply Script \"{}\" ..",path);
            executeQueryWithoutResults(new SQLQuery(content), source);
        }
    }

    public void initializeEmptyDatabase(String connectorId, String ecps, String passphrase, boolean force) throws SQLException, NoPermissionException, IOException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();
        DataSource source = dbInstance.getSource();

        SQLQuery hasPermissionsQuery = new SQLQuery("select has_database_privilege(?, ?, 'CREATE')", dbSettings.getUser(), dbSettings.getDb() );
        SQLQuery installExtensionsQuery = new SQLQuery(MaintenanceSQL.generateMandatoryExtensionSQLv2());

        SQLQuery setSearchpath = new SQLQuery(MaintenanceSQL.generateSearchPathSQL(dbSettings.getSchema()));
        SQLQuery addInitializationEntry = new SQLQuery(MaintenanceSQL.generateInitializationEntry, dbSettings.getSchema(), dbInstance.getConnectorId(), true, extensionList,
                "{\"ext\" : "+DatabaseMaintainer.XYZ_EXT_VERSION+", \"h3\" : "+DatabaseMaintainer.H3_CORE_VERSION+"}",
                null, extensionList, "{\"ext\" : "+DatabaseMaintainer.XYZ_EXT_VERSION+", \"h3\" : "+DatabaseMaintainer.H3_CORE_VERSION+"}", true, dbSettings.getSchema(), dbInstance.getConnectorId());

        boolean hasPermissions = executeQuery(hasPermissionsQuery, rs -> {
            if(rs.next())
                return rs.getBoolean(1);
            return false;
        }, source);

        if(hasPermissions){
            logger.info("Create required Extensions..");
            executeQueryWithoutResults(installExtensionsQuery, source);
            /** Create required Schemas and Tables */

            logger.info("Create required Main-Schema..");
            executeQueryWithoutResults(new SQLQuery("CREATE SCHEMA IF NOT EXISTS \"" + dbSettings.getSchema() + "\";"), source);

            logger.info("Create required Config-Schema and System-Tables..");
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.configSchemaAndSystemTablesSQL), source);

            logger.info("Create required IDX-Table..");
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.createIDXTable), source);

            logger.info("Add initial IDX Entry");
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.createIDXInitEntry), source);

            logger.info("Create required DB-Status-Table..");
            executeQueryWithoutResults(new SQLQuery(MaintenanceSQL.createDbStatusTable), source);

            /** Install extensions */
            executeLocalScripts(source, localScripts);
            /** set searchPath */
            executeQueryWithoutResults(setSearchpath, source);

            /** Mark db initialization as finished in DBStatus table*/
            logger.info("Mark db as initialized in db-status table..");
            executeQueryWithoutResults(addInitializationEntry, source);
        }else{
            throw new NoPermissionException("");
        }
    }

    public void maintainIndices(String connectorId, String ecps, String passphrase, boolean autoIndexing) throws SQLException,DecodeException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();
        DataSource source = dbInstance.getSource();
        String[] dbUser = new String[]{"wikvaya", dbSettings.getUser()};
        String maintenanceJobId = ""+Core.currentTimeMillis();
        int mode = autoIndexing == true ? 2 : 0;

        SQLQuery updateDBStatus= new SQLQuery(MaintenanceSQL.updateConnectorStatusBeginMaintenance, maintenanceJobId ,maintenanceJobId, maintenanceJobId, dbSettings.getSchema(),  dbInstance.connectorId);
        executeUpdate(updateDBStatus, source);
        try {
            SQLQuery triggerIndexing = new SQLQuery(MaintenanceSQL.createIDX, dbSettings.getSchema(), 100, 0, mode, dbUser);

            logger.info("Start Indexing..");
            executeQueryWithoutResults(triggerIndexing, source);
        }finally {
            updateDBStatus= new SQLQuery(MaintenanceSQL.updateConnectorStatusMaintenanceComplete, maintenanceJobId, maintenanceJobId, dbInstance.getDbSettings().getSchema(), dbInstance.connectorId);
            logger.info("Mark Indexing as finished");
            executeUpdate(updateDBStatus, source);
        }
    }

    public SpaceStatus getMaintenanceStatusOfSpace(String connectorId, String ecps, String passphrase, String spaceId) throws SQLException,DecodeException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();

        SQLQuery statusQuery = new SQLQuery(MaintenanceSQL.getIDXStatus,dbSettings.getSchema(), spaceId);
        logger.info("Start maintaining space '{}'..",spaceId);

        return executeQuery(statusQuery, rs -> {
            if (rs.next()) {
                try {
                    return XyzSerializable.deserialize(rs.getString(1), SpaceStatus.class);
                } catch (JsonProcessingException e) {
                    throw new RuntimeException("Cant serialize SpaceStatus");
                }
            }
            return null;
        }, dbInstance.getSource());
    }

    public void maintainSpace(String connectorId, String ecps, String passphrase, String spaceId) throws SQLException,DecodeException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();
        DataSource source = dbInstance.getSource();

        SQLQuery updateIDXEntry = new SQLQuery(MaintenanceSQL.updateIDXEntry, dbSettings.getSchema(), spaceId);
        logger.info("Set idx_creation_finished=NULL {}",spaceId);
        executeQueryWithoutResults(updateIDXEntry, source);

        SQLQuery maintainSpace = new SQLQuery(MaintenanceSQL.maintainIDXOfSpace,dbSettings.getSchema(), spaceId);
        logger.info("Start maintaining space '{}'..",spaceId);
        executeQueryWithoutResults(maintainSpace, source);
    }

    public void maintainHistory(String connectorId, String ecps, String passphrase, String spaceId, int currentVersion, int maxVersionCount) throws SQLException,DecodeException {
        MaintenanceInstance dbInstance = getClient(connectorId, ecps, passphrase);
        DatabaseSettings dbSettings = dbInstance.getDbSettings();
        DataSource source = dbInstance.getSource();
        String historyTable = spaceId + "_hst";

        long v_diff = currentVersion - maxVersionCount;
        if(v_diff >= 0) {
            SQLQuery q = new SQLQuery(SQLQueryBuilder.deleteOldHistoryEntries(dbSettings.getSchema(), historyTable , v_diff));
            q.append(new SQLQuery(SQLQueryBuilder.flagOutdatedHistoryEntries(dbSettings.getSchema(), historyTable, v_diff)));
            q.append(new SQLQuery(SQLQueryBuilder.deleteHistoryEntriesWithDeleteFlag(dbSettings.getSchema(), historyTable)));
            logger.info("Start maintaining history '{}'..",spaceId);
            executeQueryWithoutResults(q, source);
        }
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
                logger.debug("Config already loaded -> load dbInstance from Pool. DbInstanceMap size:{}", dbInstanceMap.size());
                return dbInstanceMap.get(connectorId);
            }
        }

        /** Init dataSource, readDataSource ..*/
        logger.info("{} Config is missing -> add new dbInstance to Pool. DbInstanceMap size:{}", dbInstanceMap.size());
        final ComboPooledDataSource source = getComboPooledDataSource(dbSettings, connectorId , false);

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
                logger.warn("Error while closing connections: ", e);
            }
            dbInstanceMap.remove(connectorId);
        }
    }

    private ComboPooledDataSource getComboPooledDataSource(DatabaseSettings dbSettings, String applicationName, boolean useReplica) {
        final ComboPooledDataSource cpds = new ComboPooledDataSource();

        cpds.setJdbcUrl(
                String.format("jdbc:postgresql://%1$s:%2$d/%3$s?ApplicationName=%4$s&tcpKeepAlive=true",
                        useReplica ? dbSettings.getReplicaHost() : dbSettings.getHost(),
                        dbSettings.getPort(), dbSettings.getDb(), applicationName));

        cpds.setUser(dbSettings.getUser());
        cpds.setPassword(dbSettings.getPassword());

        cpds.setInitialPoolSize(PsqlHttpVerticle.DB_INITIAL_POOL_SIZE);
        cpds.setMinPoolSize(PsqlHttpVerticle.DB_MIN_POOL_SIZE);
        cpds.setMaxPoolSize(PsqlHttpVerticle.DB_MAX_POOL_SIZE);

        cpds.setAcquireRetryAttempts(PsqlHttpVerticle.DB_ACQUIRE_RETRY_ATTEMPTS);
        cpds.setAcquireIncrement(PsqlHttpVerticle.DB_ACQUIRE_INCREMENT);

        cpds.setCheckoutTimeout(PsqlHttpVerticle.DB_CHECKOUT_TIMEOUT * 1000 );
        cpds.setTestConnectionOnCheckout(PsqlHttpVerticle.DB_TEST_CONNECTION_ON_CHECKOUT);

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
                runner.execute(c, "SET statement_timeout = " + (STATEMENT_TIMEOUT * 1000) + " ;");
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
