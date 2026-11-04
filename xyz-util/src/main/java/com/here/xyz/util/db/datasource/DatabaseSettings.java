/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.util.db.datasource;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.Payload;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.pg.InstalledScripts;
import com.here.xyz.util.db.pg.Script;
import com.here.xyz.util.db.pg.ScriptInstaller;
import com.here.xyz.util.db.pg.ScriptInstaller.Outcome;
import com.here.xyz.util.runtime.FunctionRuntime;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatabaseSettings extends Payload {
    private static final Logger logger = LogManager.getLogger(DatabaseSettings.class);
    private static final int SCRIPT_VERSIONS_TO_KEEP = 5;
    private static final long SCRIPT_CHECK_RETRY_INTERVAL_MS = 30_000;
    /**
     * The script schemas per settings ID and software version, once they were verified or a permanent error occurred
     */
    private static final Map<String, List<String>> RESOLVED_SCRIPT_SCHEMAS = new ConcurrentHashMap<>();
    /**
     * The last snapshot of the installed scripts that could be loaded per settings ID
     */
    private static final Map<String, InstalledScripts> LAST_SCRIPT_SNAPSHOT = new ConcurrentHashMap<>();
    /**
     * The time per settings ID before which no script check is done after a transient failure
     */
    private static final Map<String, Long> SCRIPT_CHECK_RETRY_AFTER = new ConcurrentHashMap<>();
    /**
     * The IDs of the scripts per settings ID and software version, whose installation failed with a permanent error in a check that
     * still has to be retried because of a transient failure. The retries do not attempt them again.
     */
    private static final Map<String, Set<String>> PERMANENTLY_FAILED_SCRIPTS = new ConcurrentHashMap<>();
    private volatile boolean initialized = false;

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
     * The maximal amount of concurrent connections, default is one, normally only increased for embedded lambda.
     * Please use dbMaxPoolSize (ConnectorParameters) instead.
     */
    @Deprecated
    public static final String PSQL_MAX_CONN = "PSQL_MAX_CONN";

    /**
     * A unique ID for this settings object.
     * Even if some of the settings of this object are changed, this ID should not change.
     * Currently, this ID is being used to identify all versions of this object in the Data Sources cache.
     */
    private String id;

    /**
     * Connection settings
     */
    private String user;
    private String password;
    private String host;
    private String replicaHost;
    private String replicaUser;
    private String db = "postgres";
    private String schema = "public";
    private int port = 5432;
    private String applicationName;
    private List<String> searchPath;
    private List<ScriptResourcePath> scriptResourcePaths;

    /**
     * Connection Pool settings
     */
    private int dbInitialPoolSize = 1;
    private int dbMinPoolSize = 1;
    private int dbMaxPoolSize = 1;
    private int dbAcquireIncrement = 1;
    private int dbAcquireRetryAttempts = 5;
    private int dbCheckoutTimeout = 7_000;
    private int dbAcquireRetryDelay = 1_000;
    private boolean dbTestConnectionOnCheckout = true;
    private int dbMaxIdleTime;
    private int statementTimeoutSeconds = 23;

    /**
     * Script check settings
     */
    private int scriptCheckTimeoutSeconds = 5;
    private int scriptInstallTimeoutSeconds = 15;

    private DatabaseSettings() {}

    public DatabaseSettings(String id) {
        this.id = id;
    }

    public DatabaseSettings(String id, Map<String, Object> databaseSettings) {
        this(id);
        setValuesFromMap(databaseSettings);
    }

    public String getId() {
        return id;
    }

    //TODO: Use DatabaseSettings model directly as sub-object in connector params instead of using obsolete env-var names mapping
    private void setValuesFromMap(Map<String, Object> databaseSettings) {
      for (String key : databaseSettings.keySet()) {
        switch (key) {
          case PSQL_DB: setDb((String) databaseSettings.get(PSQL_DB));
            break;
          case PSQL_HOST: setHost((String) databaseSettings.get(PSQL_HOST));
            break;
          case PSQL_PASSWORD: setPassword((String) databaseSettings.get(PSQL_PASSWORD));
            break;
          case PSQL_PORT: setPort(Integer.parseInt((String) databaseSettings.get(PSQL_PORT)));
            break;
          case PSQL_REPLICA_HOST: setReplicaHost((String) databaseSettings.get(PSQL_REPLICA_HOST));
            break;
          case PSQL_REPLICA_USER: setReplicaUser((String) databaseSettings.get(PSQL_REPLICA_USER));
            break;
          case PSQL_SCHEMA: setSchema((String) databaseSettings.get(PSQL_SCHEMA));
            break;
          case PSQL_USER: setUser((String) databaseSettings.get(PSQL_USER));
            break;
          case PSQL_MAX_CONN: dbMaxPoolSize = (int) databaseSettings.get(PSQL_MAX_CONN);
            break;
        }
      }
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public DatabaseSettings withUser(String user) {
        setUser(user);
        return this;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public DatabaseSettings withPassword(String password) {
        setPassword(password);
        return this;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public DatabaseSettings withHost(String host) {
        setHost(host);
        return this;
    }

    public String getReplicaHost() {
        if (replicaHost == null)
            return host;
        return replicaHost;
    }

    public void setReplicaHost(String replicaHost) {
        this.replicaHost = replicaHost;
    }

    public DatabaseSettings withReplicaHost(String replicaHost) {
        setReplicaHost(replicaHost);
        return this;
    }

    public boolean hasReplica() {
        return replicaHost != null;
    }

    public String getReplicaUser() {
        if (replicaUser == null)
            return user;
        return replicaUser;
    }

    public void setReplicaUser(String replicaUser) {
        this.replicaUser = replicaUser;
    }

    public DatabaseSettings withReplicaUser(String replicaUser) {
        setReplicaUser(replicaUser);
        return this;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public DatabaseSettings withDb(String db) {
        setDb(db);
        return this;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public DatabaseSettings withSchema(String schema) {
        setSchema(schema);
        return this;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public DatabaseSettings withPort(int port) {
        setPort(port);
        return this;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public DatabaseSettings withApplicationName(String applicationName) {
        setApplicationName(applicationName);
        return this;
    }

    public List<String> getSearchPath() {
        return searchPath;
    }

    public void setSearchPath(List<String> searchPath) {
        this.searchPath = searchPath;
    }

    public DatabaseSettings withSearchPath(List<String> searchPath) {
        setSearchPath(searchPath);
        return this;
    }

    public List<ScriptResourcePath> getScriptResourcePaths() {
        return scriptResourcePaths;
    }

    public void setScriptResourcePaths(List<ScriptResourcePath> scriptResourcePaths) {
        this.scriptResourcePaths = scriptResourcePaths;
    }

    public DatabaseSettings withScriptResourcePaths(List<ScriptResourcePath> scriptResourcePaths) {
        setScriptResourcePaths(scriptResourcePaths);
        return this;
    }

    public int getDbInitialPoolSize() {
        return dbInitialPoolSize;
    }

    public void setDbInitialPoolSize(int dbInitialPoolSize) {
        this.dbInitialPoolSize = dbInitialPoolSize;
    }

    public DatabaseSettings withDbInitialPoolSize(int dbInitialPoolSize) {
        setDbInitialPoolSize(dbInitialPoolSize);
        return this;
    }

    public int getDbMinPoolSize() {
        return dbMinPoolSize;
    }

    public void setDbMinPoolSize(int dbMinPoolSize) {
        this.dbMinPoolSize = dbMinPoolSize;
    }

    public DatabaseSettings withDbMinPoolSize(int dbMinPoolSize) {
        setDbMinPoolSize(dbMinPoolSize);
        return this;
    }

    public int getDbMaxPoolSize() {
        return dbMaxPoolSize;
    }

    public void setDbMaxPoolSize(int dbMaxPoolSize) {
        this.dbMaxPoolSize = dbMaxPoolSize;
    }

    public DatabaseSettings withDbMaxPoolSize(int dbMaxPoolSize) {
        setDbMaxPoolSize(dbMaxPoolSize);
        return this;
    }

    public int getDbAcquireIncrement() {
        return dbAcquireIncrement;
    }

    public void setDbAcquireIncrement(int dbAcquireIncrement) {
        this.dbAcquireIncrement = dbAcquireIncrement;
    }

    public DatabaseSettings withDbAcquireIncrement(int dbAcquireIncrement) {
        setDbAcquireIncrement(dbAcquireIncrement);
        return this;
    }

    public int getDbAcquireRetryAttempts() {
        return dbAcquireRetryAttempts;
    }

    public void setDbAcquireRetryAttempts(int dbAcquireRetryAttempts) {
        this.dbAcquireRetryAttempts = dbAcquireRetryAttempts;
    }

    public DatabaseSettings withDbAcquireRetryAttempts(int dbAcquireRetryAttempts) {
        setDbAcquireRetryAttempts(dbAcquireRetryAttempts);
        return this;
    }

    public int getDbAcquireRetryDelay() {
       return dbAcquireRetryDelay;
    }

    public void setDbAcquireRetryDelay(int dbAcquireRetryDelay) {
        this.dbAcquireRetryDelay = dbAcquireRetryDelay;
    }

    public DatabaseSettings withDbAcquireRetryDelay(int dbAcquireRetryDelay) {
        setDbAcquireRetryDelay(dbAcquireRetryDelay);
        return this;
    }

    public int getDbCheckoutTimeout() {
        return dbCheckoutTimeout;
    }

    public void setDbCheckoutTimeout(int dbCheckoutTimeout) {
        this.dbCheckoutTimeout = dbCheckoutTimeout;
    }

    public DatabaseSettings withDbCheckoutTimeout(int dbCheckoutTimeout) {
        setDbCheckoutTimeout(dbCheckoutTimeout);
        return this;
    }

    public boolean isDbTestConnectionOnCheckout() {
        return dbTestConnectionOnCheckout;
    }

    public void setDbTestConnectionOnCheckout(boolean dbTestConnectionOnCheckout) {
        this.dbTestConnectionOnCheckout = dbTestConnectionOnCheckout;
    }

    public DatabaseSettings withDbTestConnectionOnCheckout(boolean dbTestConnectionOnCheckout) {
        setDbTestConnectionOnCheckout(dbTestConnectionOnCheckout);
        return this;
    }

    public int getDbMaxIdleTime() {
        return dbMaxIdleTime;
    }

    public void setDbMaxIdleTime(int dbMaxIdleTime) {
        this.dbMaxIdleTime = dbMaxIdleTime;
    }

    public DatabaseSettings withDbMaxIdleTime(int dbMaxIdleTime) {
        setDbMaxIdleTime(dbMaxIdleTime);
        return this;
    }

    public int getStatementTimeoutSeconds() {
        return statementTimeoutSeconds;
    }

    public void setStatementTimeoutSeconds(int statementTimeoutSeconds) {
        this.statementTimeoutSeconds = statementTimeoutSeconds;
    }

    public DatabaseSettings withStatementTimeoutSeconds(int statementTimeoutSeconds) {
        setStatementTimeoutSeconds(statementTimeoutSeconds);
        return this;
    }

    /**
     * @return The timeout in seconds to connect for the script check and for the check query
     */
    public int getScriptCheckTimeoutSeconds() {
        return scriptCheckTimeoutSeconds;
    }

    public void setScriptCheckTimeoutSeconds(int scriptCheckTimeoutSeconds) {
        this.scriptCheckTimeoutSeconds = scriptCheckTimeoutSeconds;
    }

    public DatabaseSettings withScriptCheckTimeoutSeconds(int scriptCheckTimeoutSeconds) {
        setScriptCheckTimeoutSeconds(scriptCheckTimeoutSeconds);
        return this;
    }

    /**
     * @return The statement and lock timeout in seconds for each statement that installs or uninstalls a script schema
     */
    public int getScriptInstallTimeoutSeconds() {
        return scriptInstallTimeoutSeconds;
    }

    public void setScriptInstallTimeoutSeconds(int scriptInstallTimeoutSeconds) {
        this.scriptInstallTimeoutSeconds = scriptInstallTimeoutSeconds;
    }

    public DatabaseSettings withScriptInstallTimeoutSeconds(int scriptInstallTimeoutSeconds) {
        setScriptInstallTimeoutSeconds(scriptInstallTimeoutSeconds);
        return this;
    }

    @JsonIgnore
    private String getApplicationNameForJdbcUrl() {
        return getApplicationName() + "[" + getId() + "_" + getCacheKey() + "]";
    }

    public String getJdbcUrl(boolean useReplica) {
        return "jdbc:postgresql://" + (useReplica ? getReplicaHost() : getHost()) + ":" + getPort() + "/" + getDb() + "?ApplicationName="
            + getApplicationNameForJdbcUrl() + "&tcpKeepAlive=true";
    }

    @Deprecated
    public boolean runsLocal(){
        if(host.equals("postgres") || host.equals("localhost"))
            return true;
        return false;
    }

    @JsonIgnore
    public String getCacheKey() {
        try {
            return Hasher.getHash(getCacheString());
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException("Error creating cache key for DatabaseSettings.", e);
        }
    }

    @Override
    public String toString() {
        return "DatabaseSettings{"
            + "user='" + getUser() + "', "
            + "host='" + getHost() + "', "
            + "replicaHost='" + getReplicaHost() + "', "
            + "db='" + getDb() + "', "
            + "schema='" + getSchema() + "', "
            + "port=" + getPort()
            + "}";
    }

    /**
     * Checks whether the expected version of all SQL scripts is installed on the DB, installs missing ones and sets all
     * script schemas for the use in the search path.
     * The check runs once per JVM, settings ID and software version on one connection, normally with one query. After a
     * transient failure, the check is repeated at most every {@link #SCRIPT_CHECK_RETRY_INTERVAL_MS} ms and the search path
     * meanwhile falls back to the newest installed versions known.
     */
    private synchronized void checkScripts() {
        if (scriptResourcePaths == null || scriptResourcePaths.isEmpty())
            return;

        FunctionRuntime runtime = FunctionRuntime.getInstance();
        String softwareVersion = runtime == null ? null : runtime.getSoftwareVersion();
        String checkKey = getId() + "|" + softwareVersion;
        List<String> scriptSchemas = RESOLVED_SCRIPT_SCHEMAS.get(checkKey);
        if (scriptSchemas == null)
            scriptSchemas = checkAndInstallScripts(checkKey, softwareVersion, runtime == null ? null : runtime::getRemainingTime);

        List<String> extendedSearchPath = new ArrayList<>(getSearchPath() == null ? List.of() : getSearchPath());
        extendedSearchPath.addAll(scriptSchemas);
        logger.info("Set searchPath for connector {} to... {}", getId(), extendedSearchPath);
        setSearchPath(extendedSearchPath);
    }

    private List<String> checkAndInstallScripts(String checkKey, String softwareVersion, IntSupplier remainingMillis) {
        Long retryAfter = SCRIPT_CHECK_RETRY_AFTER.get(getId());
        if (retryAfter != null && System.currentTimeMillis() < retryAfter) {
            logger.info("Skipping the script check for connector {} until {} after a transient failure.", getId(), retryAfter);
            return ScriptInstaller.resolveSchemas(loadScripts(null, softwareVersion), softwareVersion, LAST_SCRIPT_SNAPSHOT.get(getId()));
        }

        logger.info("Checking scripts for connector {} ...", getId());
        try (SingleConnectionDataSources dataSourceProvider = new SingleConnectionDataSources(getScriptInstallationSettings(),
            getScriptCheckTimeoutSeconds(), getScriptInstallTimeoutSeconds() + 5)) {
            List<Script> scripts = loadScripts(dataSourceProvider, softwareVersion);
            Set<String> permanentlyFailedScripts = PERMANENTLY_FAILED_SCRIPTS.getOrDefault(checkKey, Set.of());
            Outcome outcome;
            try {
                outcome = ScriptInstaller.checkAndInstall(scripts, dataSourceProvider, softwareVersion, SCRIPT_VERSIONS_TO_KEEP,
                    getScriptInstallTimeoutSeconds(), remainingMillis, permanentlyFailedScripts);
            }
            catch (RuntimeException e) {
                logger.error("Error checking / installing scripts.", e);
                outcome = new Outcome(ScriptInstaller.State.TRANSIENT, null, 0, null, permanentlyFailedScripts);
            }

            if (outcome.snapshot() != null)
                LAST_SCRIPT_SNAPSHOT.put(getId(), outcome.snapshot());

            if (outcome.state() == ScriptInstaller.State.TRANSIENT) {
                SCRIPT_CHECK_RETRY_AFTER.put(getId(), System.currentTimeMillis() + SCRIPT_CHECK_RETRY_INTERVAL_MS);
                //The retry must not repeat the installations that failed permanently
                if (outcome.permanentlyFailedScripts().isEmpty())
                    PERMANENTLY_FAILED_SCRIPTS.remove(checkKey);
                else
                    PERMANENTLY_FAILED_SCRIPTS.put(checkKey, outcome.permanentlyFailedScripts());
                //Fall back to the newest installed versions of the last snapshot that could be loaded, if any
                return outcome.snapshot() != null ? outcome.schemas()
                    : ScriptInstaller.resolveSchemas(scripts, softwareVersion, LAST_SCRIPT_SNAPSHOT.get(getId()));
            }

            SCRIPT_CHECK_RETRY_AFTER.remove(getId());
            PERMANENTLY_FAILED_SCRIPTS.remove(checkKey);
            RESOLVED_SCRIPT_SCHEMAS.put(checkKey, outcome.schemas());
            return outcome.schemas();
        }
    }

    private List<Script> loadScripts(DataSourceProvider dataSourceProvider, String softwareVersion) {
        try {
            List<Script> scripts = new ArrayList<>();
            for (ScriptResourcePath scriptResourcePath : scriptResourcePaths)
                scripts.addAll(Script.loadScripts(scriptResourcePath, dataSourceProvider, softwareVersion));
            return scripts;
        }
        catch (IOException | URISyntaxException e) {
            throw new RuntimeException("Error reading script resources.", e);
        }
    }

    /**
     * Resets the state of all script checks in this JVM. Only to be used by tests.
     */
    static void resetScriptCheckState() {
        RESOLVED_SCRIPT_SCHEMAS.clear();
        LAST_SCRIPT_SNAPSHOT.clear();
        SCRIPT_CHECK_RETRY_AFTER.clear();
        PERMANENTLY_FAILED_SCRIPTS.clear();
    }

    /**
     * @return The time before which no script check is done for the provided settings ID, <code>null</code> if there is none.
     *  Only to be used by tests.
     */
    static Long getScriptCheckRetryAfter(String settingsId) {
        return SCRIPT_CHECK_RETRY_AFTER.get(settingsId);
    }

    /**
     * Ends the retry interval after a transient failure for the provided settings ID now, if there is one.
     * Only to be used by tests.
     */
    static void expireScriptCheckRetry(String settingsId) {
        SCRIPT_CHECK_RETRY_AFTER.computeIfPresent(settingsId, (id, retryAfter) -> System.currentTimeMillis());
    }


    /**
     * Provides the settings to be used for the connection that checks & installs the SQL scripts.
     * Installing the scripts requires write access. Subclasses which route their connections to a read-only host (e.g., a
     * replica) have to override this method and return settings that point to the writer. Otherwise, the installation fails
     * and the search path silently falls back to an older installed script version.
     *
     * NOTE: The returned settings are only used to connect, so they must not define any script resource paths.
     *  Otherwise, their own initialization would check & install the scripts once more.
     *
     * @return The settings to be used for checking & installing the SQL scripts
     */
    protected DatabaseSettings getScriptInstallationSettings() {
      return this;
    }

  /**
     * Must be called whenever this DatabaseSettings objects is used to initialize a new {@link DataSourceProvider}.
     */
    synchronized void init() {
        if (!initialized) {
            initialized = true;
            checkScripts();
        }
    }

    public record ScriptResourcePath(String path, String schemaPrefix, String initScript) {
        public ScriptResourcePath(String path) {
            this(path, null);
        }

        public ScriptResourcePath(String path, String schemaPrefix) {
            this(path, schemaPrefix, null);
        }
    }
}
