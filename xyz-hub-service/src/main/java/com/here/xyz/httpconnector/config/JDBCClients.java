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
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.tools.ECPSTool;
import com.mchange.v3.decode.CannotDecodeException;
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

    public static final String CONFIG_CLIENT_ID = "config_client";

    private static final Logger logger = LogManager.getLogger();
    private static final int STATUS_AND_CONFIG_CLIENTS_MAX_POOL_SIZE = 4;
    private static final String APPLICATION_NAME_PREFIX = "job_engine_";
    private static final String STATUS_CLIENT_SUFFIX = "_status";
    private static volatile Map<String, DBClient> clients = new HashMap<>();

    private static void addClients(String id, DatabaseSettings settings) {
        addClient(id, settings);
        if (!id.equalsIgnoreCase(CONFIG_CLIENT_ID))
            addClient(getStatusClientId(id), settings);
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
                .setUser(settings.getUser())
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

    private static void removeClient(String id){
        if(clients == null || clients.get(id) == null)
            return;
        logger.info("Remove SQL-Client [{}]", id);
        clients.get(id).getClient().close();
        clients.remove(id);
    }

    private static void removeClients(String id){
        removeClient(id);
        removeClient(getStatusClientId(id));
    }

    public static Set<String> getClientList(){
        if(clients == null)
            return new HashSet<>();
        return clients.keySet();
    }

    private static String getApplicationName(String clientId){
        String applicationName = APPLICATION_NAME_PREFIX + clientId + "_"+Core.START_TIME;
        if(applicationName.length() > 63)
            return applicationName.substring(0, 63);
        return applicationName;
    }

    private static String getStatusClientId(String clientId) {
        return clientId + STATUS_CLIENT_SUFFIX;
    }

    private static boolean isStatusClientId(String clientId){
        return clientId.endsWith(STATUS_CLIENT_SUFFIX);
    }

    private static int getDBPoolSize(String clientId){
        if(isStatusClientId(clientId) || clientId.equalsIgnoreCase(CONFIG_CLIENT_ID))
            return STATUS_AND_CONFIG_CLIENTS_MAX_POOL_SIZE;
        return CService.configuration.JOB_DB_POOL_SIZE_PER_CLIENT != null ? CService.configuration.JOB_DB_POOL_SIZE_PER_CLIENT : 10;
    }

    private static Future<RunningQueryStatistics> collectRunningQueries(String clientID){
        SQLQuery q = new SQLQuery(
                "select '!ignore!' as ignore, datname,pid,state,backend_type,query_start,state_change,application_name,query from pg_stat_activity "
                        +"WHERE 1=1 "
                        +"AND application_name=#{applicationName} "
                        +"AND datname=#{db} "
                        +"AND state='active' "
                        +"AND backend_type='client backend' "
                        +"AND POSITION('!ignore!' in query) = 0"
        );

        q.setNamedParameter("applicationName", getApplicationName(clientID));
        q.setNamedParameter("db", getStatusClientDatabaseSettings(clientID).getDb());
        q = q.substituteAndUseDollarSyntax(q);

        return getStatusClient(clientID)
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

    public static Future<Void> addClientIfRequired(String connectorId) {
        return HubWebClient.getConnectorConfig(connectorId)
            .compose(connector -> {
                try {
                    if (connector.params != null && connector.params.get("ecps") != null) {
                        addClientIfRequired(connectorId, (String) connector.params.get("ecps"));
                        return Future.succeededFuture();
                    }
                    else
                        return Future.failedFuture("ECPS is missing! " + connectorId);
                }
                catch (Exception e) {
                    return Future.failedFuture("Cant load dbClients for " + connectorId);
                }
            });
    }

    public static void addClientIfRequired(String id, String ecps) throws CannotDecodeException, UnsupportedOperationException {
        DatabaseSettings settings = ECPSTool.readDBSettingsFromECPS(ecps, CService.configuration.ECPS_PHRASE);

        if (CService.supportedConnectors != null && CService.supportedConnectors.indexOf(id) == -1)
            throw new UnsupportedOperationException();

        if (JDBCImporter.getClient(id) == null)
            addClients(id, settings);
        else if (!clients.get(id).cacheKey().equals(settings.getCacheKey(id))) {
            removeClients(id);
            addClients(id, settings);
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

    public static SqlClient getStatusClient(String id) {
        return getClient(getStatusClientId(id));
    }

    public static SqlClient getClient(String id){
        if(clients == null || clients.get(id) == null)
            return null;
        return clients.get(id).getClient();
    }

    public static DatabaseSettings getStatusClientDatabaseSettings(String id){
        return getClientDatabaseSettings(getStatusClientId(id));
    }

    public static DatabaseSettings getClientDatabaseSettings(String id){
        if(clients == null || clients.get(id) == null)
            return null;
        return clients.get(id).dbSettings;
    }

    public static Future<RDSStatus> getRDSStatus(String clientId) {
        return Future.succeededFuture()
            .compose(v -> {
                if (getStatusClient(clientId) == null) {
                    logger.info("DB-Client not Ready, adding one ... [{}]", clientId);
                    return addClientIfRequired(clientId);
                }
                return Future.succeededFuture();
            })
            .compose(v -> collectRunningQueries(clientId)) //Collect current metrics from Cloudwatch
            .map(runningQueryStatistics -> new RDSStatus(clientId,
                CService.jobCWClient.getAvg5MinRDSMetrics(CService.rdsLookupDatabaseIdentifier.get(clientId)), runningQueryStatistics))
            .onFailure(e -> logger.warn("Cant get RDS-Resources! ", e));
    }

    public static Future<Void> abortJobsByJobId(Job j)  {
        return addClientIfRequired(j.getTargetConnector())
                .compose(f -> {
                    SQLQuery q = buildAbortsJobQuery(j);
                    logger.info("job[{}] Abort Job {}: {}", j.getId(), j.getTargetSpaceId(), q.text());

                    return getStatusClient(j.getTargetConnector())
                            .query(q.text())
                            .execute()
                            .compose(row ->
                                 //Succeeded in any-case
                                 Future.succeededFuture()
                            );
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
