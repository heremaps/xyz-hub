/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
import com.here.xyz.hub.Core;
import com.here.xyz.psql.config.DatabaseSettings;
import com.here.xyz.psql.tools.ECPSTool;
import com.mchange.v3.decode.CannotDecodeException;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Map;
import java.util.Set;
import java.util.HashMap;
import java.util.HashSet;

public class JDBCClients {
    private static final Logger logger = LogManager.getLogger();
    private static final String APPLICATION_NAME_PREFIX = "job_engine_";
    private static Map<String, DBClient> clients = new HashMap<>();

    public static void addClient(String id, String ecps, String passphrase) throws CannotDecodeException {
        DatabaseSettings settings = ECPSTool.readDBSettingsFromECPS(ecps, passphrase);
        addClient(id, settings);
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
        clients.put(id, new DBClient(client,settings));
    }

    public static DatabaseSettings getDBSettings(String id){
        if(clients == null || clients.get(id) == null)
            return null;
        return clients.get(id).getDbSetting();
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

    /** Vertex SQL-Client */
    public static class DBClient{

        private SqlClient client;
        private DatabaseSettings dbSetting;

        public DBClient(SqlClient client, DatabaseSettings dbSetting) {
            this.client = client;
            this.dbSetting = dbSetting;
        }

        public SqlClient getClient() {
            return client;
        }

        public void setClient(SqlClient client) {
            this.client = client;
        }

        public DatabaseSettings getDbSetting() {
            return this.dbSetting;
        }

        public void setDbSetting(DatabaseSettings dbSetting) {
            this.dbSetting = dbSetting;
        }
    }
}
