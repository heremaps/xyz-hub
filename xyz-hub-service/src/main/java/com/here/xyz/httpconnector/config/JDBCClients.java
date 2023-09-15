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

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.jobs.Job;
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.httpconnector.util.status.RunningQueryStatistic;
import com.here.xyz.httpconnector.util.status.RunningQueryStatistics;
import com.here.xyz.httpconnector.util.web.HubWebClient;
import com.here.xyz.hub.Core;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.tools.ECPSTool;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.impl.ArrayTuple;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

/**
 * @deprecated Use standard JDBC based clients instead
 */
@Deprecated
public class JDBCClients {
    /**
     * We are using JDBCClients for import and export tasks.
     *
     * In total there a multiple clients in use:
     *
     * CONFIG_CLIENT: is getting used if we are using the JDBCJobConfigClient
     * CONNECTOR_CLIENT: per connector we have one client for fulfilling tasks (export/import/idx/trigger..)
     * STATUS_CLIENTS: per connector we have a second client which is getting used for status queries (how much job-related queries are currently running..)
     *
     * Each client has its on ConnectionPool with a different configuration of maxConnections.
     * */

    /** ConfigClient is used if job-configs are getting stored via JDBC.*/
    public static final String CONFIG_CLIENT_ID = "config_client";
    private static final int CONFIG_CLIENTS_MAX_POOL_SIZE = 4;

    private static final Logger logger = LogManager.getLogger();
    private static final String APPLICATION_NAME_PREFIX = "jobEngine";
    private static final String STATUS_CLIENT_SUFFIX = "_status";
    private static final String RO_CLIENT_SUFFIX = "_ro";
    private static final String MAINTENANCE_CLIENT_SUFFIX = "_maintenance";
    private static volatile Map<String, DBClient> clients = new HashMap<>();

    private static void addClients(String id, DatabaseSettings settings) {
        /** Add read/write client */
        addClient(id, settings);

        /** Add status client */
        addClient(getStatusClientId(id, false), settings);

        /** Add MaintenanceClient */
        addClient(getMaintenanceClient(id), settings);;

        if(settings.getReplicaHost() != null){
            /** Add status status ro-client */
            addClient(getStatusClientId(id, true), settings);

            /** Add read client, if available */
            addClient(getReadOnlyClientId(id), settings);;
        }
    }

    private static void addClient(String clientId, DatabaseSettings settings){
        Map<String, String> props = new HashMap<>();
        props.put("application_name", getApplicationName(clientId));
        props.put("search_path", settings.getSchema()+",h3,public,topology");
        props.put("options", "-c statement_timeout="+CService.configuration.DB_STATEMENT_TIMEOUT_IN_S * 1000);

        PgConnectOptions connectOptions = new PgConnectOptions()
                .setPort(settings.getPort())
                .setHost(settings.getHost())
                .setDatabase(settings.getDb())
                .setUser((isReadOnlyClientId(clientId) && settings.getReplicaUser() != null) ? settings.getReplicaUser() : settings.getUser())
                .setPassword(settings.getPassword())
                .setConnectTimeout(CService.configuration.DB_CHECKOUT_TIMEOUT  * 1000)
                .setReconnectAttempts(CService.configuration.DB_ACQUIRE_RETRY_ATTEMPTS)
                .setReconnectInterval(1000)
                /** Disable Pipelining */
                .setPipeliningLimit(1)
                .setProperties(props);

        PoolOptions poolOptions = new PoolOptions()
                .setMaxSize(getDBPoolSize(clientId));

        SqlClient client = PgPool.client(CService.vertx, connectOptions, poolOptions);
        logger.info("Add new SQL-Client [{}]",clientId);
        clients.put(clientId, new DBClient(client,settings,clientId));
    }

    private static DatabaseSettings getDBSettings(String id){
        if(clients == null || clients.get(id) == null)
            return null;
        return clients.get(id).getDbSettings();
    }

    protected static void removeClient(String id){
        if(clients == null || clients.get(id) == null)
            return;
        logger.info("Remove SQL-Client [{}]", id);
        clients.get(id).getClient().close();
        clients.remove(id);
    }

    protected static void removeClients(String id){
        removeClient(id);
        removeClient(getReadOnlyClientId(id));
        removeClient(getStatusClientId(id, true));
        removeClient(getStatusClientId(id, false));
    }

    public static Set<String> getClientList(){
        if(clients == null)
            return new HashSet<>();
        return clients.keySet();
    }

    private static String getApplicationName(String clientId){
        String applicationName = APPLICATION_NAME_PREFIX + Core.START_TIME +"#"+ clientId;
        if(applicationName.length() > 63)
            return applicationName.substring(0, 63);
        return applicationName;
    }

    private static String getStatusClientId(String clientId, boolean readonly) {
        return readonly ?  (getReadOnlyClientId(clientId) + STATUS_CLIENT_SUFFIX) : (clientId + STATUS_CLIENT_SUFFIX);
    }

    private static String getReadOnlyClientId(String clientId){ return clientId + RO_CLIENT_SUFFIX; }

    private static String getMaintenanceClient(String clientId) {
        return clientId + MAINTENANCE_CLIENT_SUFFIX;
    }

    private static boolean isStatusClientId(String clientId){ return clientId.endsWith(STATUS_CLIENT_SUFFIX); }

    private static boolean isReadOnlyClientId(String clientId){
        return clientId.endsWith(RO_CLIENT_SUFFIX);
    }

    private static boolean isMaintenanceClient(String clientId){
        return clientId.endsWith(MAINTENANCE_CLIENT_SUFFIX);
    }

    private static boolean isConfigClient(String clientId){
        return clientId.equalsIgnoreCase(CONFIG_CLIENT_ID);
    }

    private static int getDBPoolSize(String clientId){
        if(isConfigClient(clientId))
            return CONFIG_CLIENTS_MAX_POOL_SIZE;
        else if(isStatusClientId(clientId))
            return CService.configuration.JOB_DB_POOL_SIZE_PER_STATUS_CLIENT;
        else if(isMaintenanceClient(clientId))
            return CService.configuration.JOB_DB_POOL_SIZE_PER_MAINTENANCE_CLIENT;

        return CService.configuration.JOB_DB_POOL_SIZE_PER_CLIENT != null ? CService.configuration.JOB_DB_POOL_SIZE_PER_CLIENT : 10;
    }

    private static Future<RunningQueryStatistics> collectRunningQueries(String clientID){
        SQLQuery q = new SQLQuery(
                "select '!ignore!' as ignore, datname,pid,state,backend_type,query_start,state_change,application_name,query from pg_stat_activity "
                        +"WHERE 1=1 "
                        +"AND application_name like #{applicationName} "
                        +"AND datname=#{db} "
                        +"AND state='active' "
                        +"AND backend_type='client backend' "
                        +"AND POSITION('!ignore!' in query) = 0"
        );

        String applicationName = getApplicationName(clientID);
        /** Cut suffix of different clients */
        applicationName = applicationName.indexOf("_") == -1 ? applicationName :applicationName.substring(0,applicationName.lastIndexOf("_"));

        q.setNamedParameter("applicationName", applicationName+"%");
        q.setNamedParameter("db", getStatusClientDatabaseSettings(clientID, false).getDb());
        q = q.substituteAndUseDollarSyntax(q);

        return getStatusClient(clientID, false)
                .preparedQuery(q.text())
                .execute(new ArrayTuple(q.parameters()))
                .onFailure(f -> logger.warn(f))
                .map(rows -> {
                    RunningQueryStatistics statistics = new RunningQueryStatistics();
                    rows.forEach(
                            row -> statistics.addRunningQueryStatistic(new RunningQueryStatistic(
                                    row.getInteger("pid"),
                                    row.getString("state"),
                                    row.getLocalDateTime("query_start"),
                                    row.getLocalDateTime("state_change"),
                                    row.getString("query"),
                                    row.getString("application_name")
                            ))
                    );

                    return statistics;
                });
    }

    public static Future<DatabaseSettings> addClientsIfRequired(String connectorId) {
        return addClientsIfRequired(connectorId, true);
    }

    public static Future<DatabaseSettings> addClientsIfRequired(String connectorId, boolean checkIfSupported) {
        if(checkIfSupported && CService.supportedConnectors != null && CService.supportedConnectors.indexOf(connectorId) == -1)
            return Future.failedFuture(new HttpException(HttpResponseStatus.BAD_REQUEST, "Connector is not supported"));

        //* todo caching! */
        if(hasClient(connectorId))
            return Future.succeededFuture(getClientDatabaseSettings(connectorId));

        return HubWebClient.getConnectorConfig(connectorId)
            .compose(connector -> {
                try {
                    if (connector.params != null && connector.params.get("ecps") != null) {
                        DatabaseSettings settings = ECPSTool.readDBSettingsFromECPS( (String) connector.params.get("ecps") , CService.configuration.ECPS_PHRASE);
                        settings.setEnabledHashSpaceId(connector.params.get("enableHashedSpaceId") != null ? (boolean) connector.params.get("enableHashedSpaceId") : false);
                        addClientsIfRequired(connectorId, settings);
                        return Future.succeededFuture(settings);
                    }
                    else
                        return Future.failedFuture(new HttpException(HttpResponseStatus.BAD_REQUEST, "Connector ["+connectorId+"] cant get found/loaded!"));
                }
                catch (Exception e) {
                    return Future.failedFuture(new HttpException(HttpResponseStatus.BAD_REQUEST, "Cant decode ECPS of Connector ["+connectorId+"]!"));
                }
            });
    }

    public static DatabaseSettings addClientsIfRequired(String id, DatabaseSettings settings) {
        synchronized (clients){
            if (!hasClient(id))
                addClients(id, settings);
            else if (!clients.get(id).cacheKey().equals(settings.getCacheKey(id))) {
                removeClients(id);
                addClients(id, settings);
            }
            return settings;
        }
    }

    public static void addConfigClient(){
        /** Used to store Job definitions inside DB if Dynamo is not used */
        DatabaseSettings settings = new DatabaseSettings();

        if(CService.configuration != null && CService.configuration.STORAGE_DB_URL != null) {
            URI uri = URI.create(CService.configuration.STORAGE_DB_URL.substring(5));
            settings.setHost(uri.getHost());
            settings.setPort(Integer.valueOf(uri.getPort() == -1 ? 5432 : uri.getPort()));

            String[] pathComponent = uri.getPath() == null ? null : uri.getPath().split("/");
            if (pathComponent != null && pathComponent.length > 1)
                settings.setDb(pathComponent[1]);
        }
        if(CService.configuration != null && CService.configuration.STORAGE_DB_USER != null)
            settings.setUser(CService.configuration.STORAGE_DB_USER);
        if(CService.configuration != null && CService.configuration.STORAGE_DB_PASSWORD != null)
            settings.setPassword(CService.configuration.STORAGE_DB_PASSWORD);

        addClient(CONFIG_CLIENT_ID, settings);
    }

    public static String getDefaultSchema(String id){
        if(getDBSettings(id) != null)
            return getDBSettings(id).getSchema();
        return null;
    }

    public static SqlClient getStatusClient(String id, boolean readOnly) {
        return getClient(getStatusClientId(id, readOnly));
    }

    public static SqlClient getRoClient(String id){
        return getClient(getReadOnlyClientId(id));
    }

    public static SqlClient getClient(String id, boolean useReadOnlyClient){
        if(useReadOnlyClient && hasClient(getReadOnlyClientId(id)))
            return getRoClient(id);
        /** if useReadOnlyClient=true and no ro-client is available we fall back to default client */
        return getClient(id);
    }

    public static SqlClient getClient(String id){
        if(clients == null || clients.get(id) == null)
            return null;
        return clients.get(id).getClient();
    }

    public static boolean hasClient(String id){
        if(clients == null || clients.get(id) == null)
            return false;
        return true;
    }

    public static boolean hasStatusClient(String id, boolean readonly){
        return hasClient(getStatusClientId(id, readonly));
    }

    public static boolean hasROClient(String id){
        return hasClient(getReadOnlyClientId(id));
    }

    public static DatabaseSettings getStatusClientDatabaseSettings(String id, boolean readonly){
        return getClientDatabaseSettings(getStatusClientId(id,readonly));
    }

    public static DatabaseSettings getClientDatabaseSettings(String id){
        if(clients == null || clients.get(id) == null)
            return null;
        return clients.get(id).dbSettings;
    }

    public static Future<RDSStatus> getRDSStatus(String connectorId) {
        return Future.succeededFuture()
                .compose(v -> {
                    if (getStatusClient(connectorId, false) == null) {
                        logger.info("DB-Client not Ready, adding one ... [{}]", connectorId);
                        return addClientsIfRequired(connectorId);
                    }
                    return Future.succeededFuture();
                })
                .compose(v ->
                    /** collect running queries from db */
                    collectRunningQueries(connectorId)
                ).compose(runningQueryStatistics -> {
                    RDSStatus rdsStatus = new RDSStatus(connectorId);
                    rdsStatus.addRdsMetrics(runningQueryStatistics);

                    String dbClusterIdentifier = getDBSettings(connectorId).getDBClusterIdentifier();
                    if(dbClusterIdentifier == null){
                        logger.error("{} - configured ECPS does not use a clusterConfig! {}",connectorId, getDBSettings(connectorId).getHost());
                        rdsStatus.addCloudWatchDBReaderMetrics(new JSONObject());
                    }
                    else {
                        /** collect cloudwatch metrics for db */
                        CService.jobCWClient.getAvg5MinRDSMetrics(dbClusterIdentifier);
                        rdsStatus.addCloudWatchDBWriterMetrics(CService.jobCWClient.getAvg5MinRDSMetrics(dbClusterIdentifier, AwsCWClient.Role.WRITER));
                        if(getDBSettings(connectorId).getReplicaHost() == null)
                            rdsStatus.addCloudWatchDBReaderMetrics(CService.jobCWClient.getAvg5MinRDSMetrics(dbClusterIdentifier, AwsCWClient.Role.READER));
                    }

                    return Future.succeededFuture(rdsStatus);
                })
                .onFailure(e -> logger.warn("Cant get RDS-Resources! ", e));
    }

    public static Future<Void> abortJobsByJobId(Job j)  {
        SQLQuery q = buildAbortsJobQuery(j);
        logger.info("job[{}] Abort Job {}: {}", j.getId(), j.getTargetSpaceId(), q.text());

        return addClientsIfRequired(j.getTargetConnector())
                .compose(f ->
                    getStatusClient(j.getTargetConnector(), false)
                            .query(q.text())
                            .execute()
                            .compose(row ->
                                 //Succeeded in any-case
                                 Future.succeededFuture()
                            )
                ).compose(f -> {
                    /** Abort readReplica queries */
                    if(hasStatusClient(j.getTargetConnector(), true))
                        return getStatusClient(j.getTargetConnector(), true)
                                .query(q.text())
                                .execute()
                                .compose(row ->
                                        //Succeeded in any-case
                                        Future.succeededFuture()
                                );
                    return Future.succeededFuture();
                });
    }

    private static SQLQuery buildAbortsJobQuery(Job j) {
        return new SQLQuery(String.format("select pg_terminate_backend( pid ) from pg_stat_activity "
                        + "where 1 = 1 "
                        + "and state = 'active' "
                        + "and strpos( query, 'pg_terminate_backend' ) = 0 "
                        + "and strpos( query, '%s' ) > 0 "
                        + "and strpos( query, 'm499#jobId(%s)' ) > 0 ",
                j.getQueryIdentifier() ,j.getId()));
    }

    /** Vertex SQL-Client */
    private static class DBClient{
        private String connectorId;
        private SqlClient client;
        private DatabaseSettings dbSettings;

        public DBClient(SqlClient client, DatabaseSettings dbSettings, String connectorId) {
            this.client = client;
            this.dbSettings = dbSettings;
            this.connectorId = connectorId;
        }

        public SqlClient getClient() {
            return client;
        }

        public void setClient(SqlClient client) {
            this.client = client;
        }

        public DatabaseSettings getDbSettings() {
            return this.dbSettings;
        }

        public void setDbSettings(DatabaseSettings dbSettings) {
            this.dbSettings = dbSettings;
        }

        public String cacheKey(){ return dbSettings.getCacheKey(connectorId);}
    }
}
