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

package com.here.xyz.psql.config;

import com.here.xyz.util.Hasher;

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
     * A constant that is normally used as environment variable name for the replica user - if not set PSQL_USER will be used instead.
     * As password the PSQL_PASSWORD will be shared.
     */
    public static final String PSQL_REPLICA_USER = "PSQL_REPLICA_USER";

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
    private String replicaUser;
    private String db;
    private String schema;
    private Integer port;
    private Boolean readOnly;
    private Boolean enableHashedSpaceId;
    @Deprecated
    private Integer maxConnections;

    public DatabaseSettings(){
       setDefaults();
    }

    public DatabaseSettings(String schema){
        this.schema = schema;
    }

    public DatabaseSettings(PSQLConfig config) {
        super();
        this.user = config.readFromEnvVars(PSQL_USER);
        this.password = config.readFromEnvVars(PSQL_PASSWORD);
        this.host = config.readFromEnvVars(PSQL_HOST);
        this.replicaHost = config.readFromEnvVars(PSQL_REPLICA_HOST);
        this.replicaUser = config.readFromEnvVars(PSQL_REPLICA_USER);
        this.db = config.readFromEnvVars(PSQL_DB);
        this.schema = config.readFromEnvVars(PSQL_SCHEMA);
        this.port = config.readFromEnvVars(PSQL_PORT) != null ? Integer.parseInt(config.readFromEnvVars(PSQL_PORT)) : null;
        this.readOnly = config.readFromEnvVars(PSQL_READ_ONLY) != null ? Boolean.parseBoolean(config.readFromEnvVars(PSQL_READ_ONLY)) : null;
        this.maxConnections = config.readFromEnvVars(PSQL_MAX_CONN) != null ? Integer.parseInt(config.readFromEnvVars(PSQL_MAX_CONN)) : null;

        setDefaults();
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

    public void setPsqlReplicaUser(String replicaUser) {
        this.replicaUser = replicaUser;
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

    public String getHost() {return host; }

    public String getHost( boolean useReplica) {
        if(!useReplica || replicaHost == null)
            return host;
        return replicaHost;
    }

    public String getUser(boolean useReplica) {
        if(!useReplica || replicaUser == null)
            return user;
        return replicaUser;
    }

    public String getReplicaHost() { return replicaHost; }

    public String getReplicaUser() {
        return replicaUser;
    }

    public String getDb() { return db; }

    public String getSchema() {
        return schema;
    }

    public int getPort() {
        return port;
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public Boolean isEnabledHashSpaceId() { return enableHashedSpaceId;}

    public void setEnabledHashSpaceId(Boolean enableHashedSpaceId) { this.enableHashedSpaceId = enableHashedSpaceId; }

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

    public String getConfigValuesAsString() {
        return host+replicaHost+db+user+schema+port+password;
    }

    public String getCacheKey(String id) {
        return Hasher.getHash(id+getConfigValuesAsString());
    }

    public String getDBClusterIdentifier() {
        if(host == null || host.indexOf(".cluster-") == -1)
            return null;

        //DbClusterIdentifier.ClusterIdentifier.Region.rds.amazonaws.com
        return host.split("\\.")[0];
    }

    public void setDefaults(){
        if(this.user == null)
            this.user = "postgres";
        if(this.password == null)
            this.password = "password";
        if(this.host == null)
            this.host = "localhost";
        if(this.replicaHost == null)
            this.replicaHost = null;
        if(this.replicaUser == null)
            this.replicaUser = null;
        if(this.db == null)
            this.db = "postgres";
        if(this.schema == null)
            this.schema = "public";
        if(this.port == null)
            this.port = 5432;
        if(this.readOnly == null)
            this.readOnly = false;
    }
}
