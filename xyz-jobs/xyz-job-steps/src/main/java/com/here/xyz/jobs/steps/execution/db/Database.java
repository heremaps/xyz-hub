/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.jobs.steps.execution.db;

import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.READER;
import static com.here.xyz.jobs.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.util.db.datasource.DatabaseSettings.PSQL_HOST;
import static com.here.xyz.util.db.datasource.DatabaseSettings.PSQL_REPLICA_HOST;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.resources.AwsRDSClient;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.ECPSTool;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.web.HubWebClient;
import com.here.xyz.util.web.HubWebClientAsync;
import com.here.xyz.util.web.XyzWebClient.WebClientException;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Name;
import org.xbill.DNS.Record;
import software.amazon.awssdk.services.rds.model.DBCluster;

public class Database extends ExecutionResource {
  private static final List<ScriptResourcePath> SCRIPT_RESOURCE_PATHS = List.of(new ScriptResourcePath("/sql", "jobs", "common"), new ScriptResourcePath("/jobs", "jobs"));
  private static final Logger logger = LogManager.getLogger();
  private static final float DB_MAX_JOB_UTILIZATION_PERCENTAGE = 0.6f;
  private static final Pattern RDS_CLUSTER_HOSTNAME_PATTERN = Pattern.compile("(.+).cluster-.*.rds.amazonaws.com.*");
  private static Cache<String, List<Database>> cache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(3, TimeUnit.MINUTES)
      .build();
  private static final String ALL_DATABASES = "ALL_DATABASES";
  private String name;
  private DatabaseRole role;
  private String clusterId;
  private String instanceId;
  private double maxUnits;
  private Map<String, Object> connectorDbSettingsMap;
  private DatabaseSettings dbSettings;
  private DataSourceProvider usedDataSourceProvider;
  private static Set<Database> usedDbs = new HashSet<>();

  private Database(String clusterId, String instanceId, double maxUnits, Map<String, Object> connectorDbSettingsMap) {
    this.clusterId = clusterId;
    this.instanceId = instanceId;
    this.maxUnits = maxUnits;
    this.connectorDbSettingsMap = connectorDbSettingsMap;
  }

  DataSourceProvider getDataSources() {
    if (usedDataSourceProvider == null)
      usedDataSourceProvider = new PooledDataSources(getDatabaseSettings());
    usedDbs.add(this);
    return usedDataSourceProvider;
  }

  void close() {
    if (usedDataSourceProvider != null)
      try {
        usedDataSourceProvider.close();
        usedDataSourceProvider = null;
      }
      catch (Exception e) {
        logger.error("Error closing connections for database {}.", getName(), e);
      }
  }

  static void closeAll() {
    for (Database db : usedDbs)
      db.close();
    usedDbs = new HashSet<>();
  }

  DatabaseSettings getDatabaseSettings() {
    if (dbSettings == null)
      dbSettings = new RestrictedDatabaseSettings(getName(), connectorDbSettingsMap)
          .withApplicationName("JobFramework")
          .withScriptResourcePaths(SCRIPT_RESOURCE_PATHS);
    dbSettings.setStatementTimeoutSeconds(600);
    return dbSettings;
  }

  public static Future<List<Database>> getAll() {
    logger.info("Gathering all database objects ...");
    List<Database> allDbs = cache.getIfPresent(ALL_DATABASES);
    if (allDbs != null)
      return Future.succeededFuture(allDbs);

    return initializeDatabases()
        .compose(loadedDbs -> {
          cache.put(ALL_DATABASES, loadedDbs);
          return Future.succeededFuture(loadedDbs);
        });
  }

  public static Database loadDatabase(String name, DatabaseRole role) {
    try {
      List<Database> dbs = loadDatabasesForConnector(HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadConnector(name));

      return dbs.stream()
          .filter(db -> db.getName().equals(name) && role.equals(db.getRole())).findAny().get();
    }
    catch (NoSuchElementException | WebClientException e) {
      //The requested database was not found
      throw new RuntimeException("No database was found with name " + name + " and role " + role, e);
    }
  }

  protected static Database loadDatabase(String name, String id) {
    try {
      List<Database> dbs = loadDatabasesForConnector(HubWebClient.getInstance(Config.instance.HUB_ENDPOINT).loadConnector(name));

      return dbs.stream()
          .filter(db -> db.getName().equals(name) && db.getId().equals(id)).findAny().get();
    }
    catch (NoSuchElementException | WebClientException e) {
      //The requested database was not found
      throw new RuntimeException("No database was found with name " + name + " and id " + id, e);
    }
  }

  private static Future<List<Database>> initializeDatabases() {
    final HubWebClientAsync hubClient = HubWebClientAsync.getInstance(Config.instance.HUB_ENDPOINT);
    if (!hubClient.isServiceReachable())
      return Future.succeededFuture(List.of());
    return hubClient.loadConnectorsAsync()
        .compose(connectors -> {
          //TODO: Run the following asynchronously
          List<Database> allDbs = new CopyOnWriteArrayList<>();
          for (Connector connector : connectors) {
            //TODO: Add some value(s) in connector config to identify if the database can be added in the region
            allDbs.addAll(loadDatabasesForConnector(connector));
          }
          return Future.succeededFuture(allDbs);
        });
  }

  /**
   * @deprecated This method is used only as workaround for DNS resolution of connector-DB-host and will be removed soon.
   * Please do not use it for any other purposes.
   */
  @Deprecated
  private static boolean isLocal() {
    return Config.instance.LOCALSTACK_ENDPOINT != null;
  }

  private static void fixLocalDbHosts(Map<String, Object> dbSettings) {
    if (!isLocal() || Config.instance.LOCAL_DB_HOST_OVERRIDE == null)
      return;

    if (dbSettings.get(PSQL_HOST) instanceof String dbHost)
      dbSettings.put(PSQL_HOST, dbHost.replace("localhost", Config.instance.LOCAL_DB_HOST_OVERRIDE));
    if (dbSettings.get(PSQL_REPLICA_HOST) instanceof String dbHost)
      dbSettings.put(PSQL_REPLICA_HOST, dbHost.replace("localhost", Config.instance.LOCAL_DB_HOST_OVERRIDE));
  }

  private static List<Database> loadDatabasesForConnector(Connector connector) {
    logger.info("Gathering database objects for connector \"{}\" ...", connector.id);
    List<Database> databasesFromCache = cache.getIfPresent(connector.id);
    if (databasesFromCache != null)
      return databasesFromCache;

    List<Database> databases = new ArrayList<>();

    if (connector.active) {
      final ConnectorParameters connectorParameters = ConnectorParameters.fromMap(connector.params);
      if (connectorParameters.getEcps() != null) { //Ignore connectors which have no db settings
        final Map<String, Object> connectorDbSettingsMap = ECPSTool.decryptToMap(Config.instance.ECPS_PHRASE,
            connectorParameters.getEcps());
        fixLocalDbHosts(connectorDbSettingsMap);

        DatabaseSettings connectorDbSettings = new DatabaseSettings(connector.id, connectorDbSettingsMap)
            .withDbMaxPoolSize(10)
            .withScriptResourcePaths(SCRIPT_RESOURCE_PATHS);

        String rdsClusterId = getClusterIdFromHostname(connectorDbSettings.getHost());

        if (rdsClusterId == null) {
          logger.warn("No cluster ID detected for hostname of DB \"" + connector.id + "\". Taking it into account as simple writer DB.");
          databases.add(new Database(null, null, 128, connectorDbSettingsMap)
              .withName(connector.id)
              .withRole(WRITER));

          //TODO: Ensure that we always have a reader for all Databases (by using the read Only user or replica_host if present) and then - if there is none - it is not supported for a good reason
          //Adding a virtual readReplica for local testing (same db but ro user)
          if(connector.id.equals("psql") && (connectorDbSettings.runsLocal())) {
            databases.add(new Database(null, null, 128, connectorDbSettingsMap)
                    .withName(connector.id)
                    .withRole(READER));
          }
        }
        else {
          DBCluster dbCluster = AwsRDSClient.getInstance().getRDSClusterConfig(rdsClusterId);
          if(dbCluster != null) {
            dbCluster.dbClusterMembers().forEach(instance -> {
              final DatabaseRole role = instance.isClusterWriter() ? WRITER : READER;

              databases.add(new Database(rdsClusterId, instance.dbInstanceIdentifier(),
                      dbCluster.serverlessV2ScalingConfiguration().maxCapacity(), connectorDbSettingsMap)
                      .withName(connector.id)
                      .withRole(role));
            });
          }
        }
      }
    }

    //Update the cache
    updateAllDbsCacheEntry(connector, databases);
    cache.put(connector.id, databases);

    return databases;
  }

  private static void updateAllDbsCacheEntry(Connector connector, List<Database> databases) {
    List<Database> allDbs = cache.getIfPresent(ALL_DATABASES);
    if (allDbs != null) {
      for (Database db : allDbs)
        if (db.name.equals(connector.id))
          allDbs.remove(db);
      allDbs.addAll(databases);
    }
  }

  public static String getClusterIdFromHostname(String hostname) {
    if(hostname == null) return null;
    return Optional.ofNullable(extractClusterId(hostname)).orElse(resolveAndExtractClusterId(hostname));
  }

  private static String extractClusterId(String url) {
    Matcher matcher = RDS_CLUSTER_HOSTNAME_PATTERN.matcher(url);
    return matcher.matches() ? matcher.group(1) : null;
  }

  private static String resolveAndExtractClusterId(String hostname) {
    try {
      Lookup lookup = new Lookup(hostname);

      List<String> records = Arrays.stream(lookup.run()).map(Record::toString).collect(Collectors.toList());
      records.addAll(Arrays.stream(lookup.getAliases()).map(Name::toString).collect(Collectors.toList()));

      for(String record : records) {
        String clusterId = extractClusterId(record);
        if(clusterId != null) return clusterId;
      }
    } catch (Exception e) {
      // Do nothing
    }
    return null;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Database withName(String name) {
    setName(name);
    return this;
  }

  public DatabaseRole getRole() {
    return role;
  }

  public void setRole(DatabaseRole role) {
    this.role = role;
  }

  public Database withRole(DatabaseRole role) {
    setRole(role);
    return this;
  }

  @Override
  public Future<Double> getUtilizedUnits() {
    //TODO: Call CW Metric to get the current ACU utilization (see v1 implementaton)
    return null;
  }

  @Override
  protected double getMaxUnits() {
    return maxUnits;
  }

  @Override
  protected double getMaxVirtualUnits() {
    return getMaxUnits() * DB_MAX_JOB_UTILIZATION_PERCENTAGE;
  }

  @Override
  protected String getId() {
    return Hasher.getHash(getName() + getDatabaseSettings().getHost());
  }

  @Override
  public String toString() {
    return "Database{" +
        "name='" + name + '\'' +
        ", role=" + role +
        '}';
  }

  public enum DatabaseRole {
    READER,
    WRITER
  }

  /**
   * Provides access only to either the writer or the reader host of the passed database settings
   * depending on the role of the parent database instance.
   */
  private class RestrictedDatabaseSettings extends DatabaseSettings {
    public RestrictedDatabaseSettings(String id, Map<String, Object> databaseSettings) {
      super(id, databaseSettings);
    }

    @Override
    public String getUser() {
      if (role == READER)
        return super.getReplicaUser();
      return super.getUser();
    }

    @Override
    public String getHost() {
      if (role == READER)
        return super.getReplicaHost();
      return super.getHost();
    }

    @Override
    public String getReplicaHost() {
      if (role == WRITER)
        return super.getHost();
      return super.getReplicaHost();
    }

    @Override
    public boolean hasReplica() {
      return false;
    }

    @Override
    public String getReplicaUser() {
      if (role == WRITER)
        return super.getUser();
      return super.getReplicaUser();
    }
  }
}
