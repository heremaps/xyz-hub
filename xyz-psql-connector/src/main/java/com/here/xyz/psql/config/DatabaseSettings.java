/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.psql.config;

import com.amazonaws.services.lambda.runtime.Context;
import com.here.xyz.connectors.SimulatedContext;

public class DatabaseSettings {

    /**
     * A constant that is normally used as environment variable name for the host.
     */
    public static final String PSQL_HOST = "PSQL_HOST";

    /**
     * A constant that is normally used as environment variable name for the port.
     */
    public static final String PSQL_PORT = "PSQL_PORT";

    /**
     * A constant that is normally used as environment variable name for the host.
     */
    public static final String PSQL_REPLICA_HOST = "PSQL_REPLICA_HOST";

    /**
     * A constant that is normally used as environment variable name for the database.
     */
    public static final String PSQL_DB = "PSQL_DB";

    /**
     * A constant that is normally used as environment variable name for the user.
     */
    public static final String PSQL_USER = "PSQL_USER";

    /**
     * A constant that is normally used as environment variable name for the password.
     */
    public static final String PSQL_PASSWORD = "PSQL_PASSWORD";

    /**
     * A constant that is normally used as environment variable name for the schema.
     */
    public static final String PSQL_SCHEMA = "PSQL_SCHEMA";

    /**
     * A constant that is normally used as environment variable name for the schema.
     */
    public static final String PSQL_READ_ONLY = "READ_ONLY";
    /**
     * The maximal amount of concurrent connections, default is one, normally only increased for embedded lambda.
     * Please use dbMaxPoolSize (ConnectorParameters) instead.
     */
    @Deprecated
    public static final String PSQL_MAX_CONN = "PSQL_MAX_CONN";

    private String user;
    private String password;
    private String host;
    private String replicaHost;
    private String db;
    private String schema;
    private Integer port;
    private Boolean readOnly;
    @Deprecated
    private Integer maxConnections;

    public DatabaseSettings(Context context){
        this.user = readFromEnvVars(PSQL_USER,context);
        this.password = readFromEnvVars(PSQL_PASSWORD,context);
        this.host = readFromEnvVars(PSQL_HOST,context);
        this.replicaHost = readFromEnvVars(PSQL_REPLICA_HOST,context);
        this.db = readFromEnvVars(PSQL_DB,context);
        this.schema = readFromEnvVars(PSQL_SCHEMA,context);
        this.port = readFromEnvVars(PSQL_PORT,context) != null ? Integer.parseInt(readFromEnvVars(PSQL_PORT,context)) : null;
        this.readOnly = readFromEnvVars(PSQL_READ_ONLY,context) != null ? Boolean.parseBoolean( readFromEnvVars(PSQL_READ_ONLY,context)) : null;
        this.maxConnections = readFromEnvVars(PSQL_MAX_CONN,context) != null ? Integer.parseInt(readFromEnvVars(PSQL_MAX_CONN,context)) : null;

        if(this.user == null)
            this.user = "postgres";
        if(this.password == null)
            this.password = "password";
        if(this.host == null)
            this.host = "localhost";
        if(this.replicaHost == null)
            this.replicaHost = null;
        if(this.db == null)
            this.db = "postgres";
        if(this.schema == null)
            this.schema = "public";
        if(this.port == null)
            this.port = 5432;
        if(this.readOnly == null)
            this.readOnly = false;
    }

    protected static String readFromEnvVars(String name, Context context) {
        if (context instanceof SimulatedContext) {
            return ((SimulatedContext) context).getEnv(name);
        }
        return System.getenv(name);
    }

    public void setUser(String user) {
        this.user = user;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setReplicaHost(String replicaHost) {
        this.replicaHost = replicaHost;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getHost() {
        return host;
    }

    public String getReplicaHost() {
        return replicaHost;
    }

    public String getDb() {
        return db;
    }

    public String getSchema() {
        return schema;
    }

    public int getPort() {
        return port;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    @Deprecated
    public Integer getMaxConnections() {
        return maxConnections;
    }

    @Override
    public String toString() {
        return "DatabaseSettings{" +
                "user='" + user + '\'' +
                ", host='" + host + '\'' +
                ", replicaHost='" + replicaHost + '\'' +
                ", db='" + db + '\'' +
                ", schema='" + schema + '\'' +
                ", port=" + port +
                '}';
    }
}
