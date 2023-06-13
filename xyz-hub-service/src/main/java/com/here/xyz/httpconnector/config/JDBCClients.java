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
import com.here.xyz.httpconnector.util.status.RDSStatus;
import com.here.xyz.httpconnector.util.status.RunningQueryStatistic;
import com.here.xyz.httpconnector.util.status.RunningQueryStatistics;
import com.here.xyz.hub.Core;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.tools.ECPSTool;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.impl.ArrayTuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JDBCClients {
    private static final Logger logger = LogManager.getLogger();
    private static final String APPLICATION_NAME_PREFIX = "job_engine_";
    public static final String CONFIG_CLIENT_ID = "config_client";
    private static volatile Map<String, DBClient> clients = new HashMap<>();

    public static void addClientIfRequired(String id, String ecps, String passphrase) throws CannotDecodeException, UnsupportedOperationException {
        DatabaseSettings settings = ECPSTool.readDBSettingsFromECPS(ecps, passphrase);

        if(CService.supportedConnectors != null && CService.supportedConnectors.indexOf(id) == -1)
            throw new UnsupportedOperationException();

        if(JDBCImporter.getClient(id) == null){
            addClient(id, settings);
        }else{
            if(!clients.get(id).cacheKey().equals(settings.getCacheKey(id))) {
                removeClient(id);
                addClient(id, settings);
            }
        }
    }

    public static void addClient(String id, String ecps, String passphrase) throws CannotDecodeException {
        DatabaseSettings settings = ECPSTool.readDBSettingsFromECPS(ecps, passphrase);
        addClient(id, settings);
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

    static void addClient(String id, DatabaseSettings settings){
        Map<String, String> props = new HashMap<>();
        props.put("application_name", getApplicationName(id));
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
                .setMaxSize(CService.configuration.DB_MAX_POOL_SIZE);

        SqlClient client = PgPool.client(CService.vertx, connectOptions, poolOptions);
        logger.info("Add new SQL-Client [{}]",id);
        clients.put(id, new DBClient(client,settings,id));
    }

    public static DatabaseSettings getDBSettings(String id){
        if(clients == null || clients.get(id) == null)
            return null;
        return clients.get(id).getDbSettings();
    }

    public static String getDefaultSchema(String id){
        if(getDBSettings(id) != null)
            return getDBSettings(id).getSchema();
        return null;
    }

    public static SqlClient getClient(String id){
        if(clients == null || clients.get(id) == null)
            return null;
        return clients.get(id).getClient();
    }

    public static void removeClient(String id){
        if(clients == null || clients.get(id) == null)
            return;
        logger.info("Remove SQL-Client [{}]", id);
        clients.get(id).getClient().close();
        clients.remove(id);
    }

    public static Set<String> getClientList(){
        if(clients == null)
            return new HashSet<>();
        return clients.keySet();
    }

    public static String getApplicationName(String clientId){
        String applicationName = APPLICATION_NAME_PREFIX+clientId+"_"+Core.START_TIME;
        if(applicationName.length() > 63)
            return applicationName.substring(0, 63);
        return applicationName;
    }

    public static String getViewName(String tablename){
        return tablename+"_view";
    }

    public static Future<RunningQueryStatistics> collectRunningQueries(String clientID){
        SQLQuery q = new SQLQuery(
                "select '!ignore!' as ignore, datname,pid,state,backend_type,query_start,state_change,application_name,query from pg_stat_activity "
                        +"WHERE 1=1 "
                        +"AND application_name=#{applicationName} "
                        +"AND datname='postgres' "
                        +"AND state='active' "
                        +"AND backend_type='client backend' "
                        +"AND POSITION('!ignore!' in query) = 0"
        );

        q.setNamedParameter("applicationName", getApplicationName(clientID));
        q = q.substituteAndUseDollarSyntax(q);

        return getClient(clientID)
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

    public static Future<RDSStatus> getRDSStatus(String clientId) {
        Promise<RDSStatus> p = Promise.promise();
        /** Collection current metrics from Cloudwatch */

        if(JDBCImporter.getClient(clientId) == null){
            logger.info("DB-Client not Ready!");
            p.complete(null);
        }else{
            JDBCImporter.collectRunningQueries(clientId)
                    .onSuccess(runningQueryStatistics -> {
                        /** Collect metrics from Cloudwatch */
                        JSONObject avg5MinRDSMetrics = CService.jobCWClient.getAvg5MinRDSMetrics(CService.rdsLookupDatabaseIdentifier.get(clientId));
                        p.complete(new RDSStatus(clientId, avg5MinRDSMetrics, runningQueryStatistics));
                    }).onFailure(f -> {
                        logger.warn("Cant get RDS-Resources {}",f);
                        p.fail(f);
                    } );
        }

        return p.future();
    }

    /** Vertex SQL-Client */
    public static class DBClient{
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
