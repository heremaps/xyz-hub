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

package com.here.xyz.httpconnector.job.steps.execution.db;

import static com.here.xyz.httpconnector.job.steps.execution.db.Database.DatabaseRole.READER;
import static com.here.xyz.httpconnector.job.steps.execution.db.Database.DatabaseRole.WRITER;
import static com.here.xyz.httpconnector.util.web.HubWebClient.loadConnector;

import com.amazonaws.services.rds.model.DBCluster;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.job.resources.AwsRDSClient;
import com.here.xyz.httpconnector.job.resources.ExecutionResource;
import com.here.xyz.httpconnector.util.web.HubWebClient.HubWebClientException;
import com.here.xyz.httpconnector.util.web.HubWebClientAsync;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.psql.config.ConnectorParameters;
import com.here.xyz.psql.tools.ECPSTool;
import com.here.xyz.util.db.DatabaseSettings;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.PooledDataSources;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xbill.DNS.Lookup;
import org.xbill.DNS.Name;
import org.xbill.DNS.Record;

public class Database extends ExecutionResource {
  private static final Logger logger = LogManager.getLogger();
  private static final float DB_MAX_JOB_UTILIZATION_PERCENTAGE = 0.6f;
  private static final Pattern RDS_CLUSTER_HOSTNAME_PATTERN = Pattern.compile("(.+).cluster-.*.rds.amazonaws.com.*");
  private static Cache<String, List<Database>> cache = CacheBuilder
      .newBuilder()
      .expireAfterWrite(3, TimeUnit.MINUTES)
      .build();
  private String name;
  private DatabaseRole role;
  private String clusterId;
  private String instanceId;
  private double maxUnits;
  private Map<String, Object> connectorDbSettingsMap;
  private DatabaseSettings dbSettings;

  private Database(String clusterId, String instanceId, double maxUnits, Map<String, Object> connectorDbSettingsMap) {
    this.clusterId = clusterId;
    this.instanceId = instanceId;
    this.maxUnits = maxUnits;
    this.connectorDbSettingsMap = connectorDbSettingsMap;
  }

  DataSourceProvider getDataSources() {
    return new PooledDataSources(getDatabaseSettings());
  }

  DatabaseSettings getDatabaseSettings() {
    if (dbSettings == null)
      dbSettings = new RestrictedDatabaseSettings(getName(), connectorDbSettingsMap);
    return dbSettings;
  }

  //TODO: Needed at all?
  public static List<Database> getAll() {
    //TODO: Call initializeDatabases internally here and cache it in a static guava cache internally for some time
    return null;
  }

  public static Database loadDatabase(String name, DatabaseRole role) {
    try {
      List<Database> dbs = loadDatabasesForConnector(loadConnector(name));

      return dbs.stream()
          .filter(db -> db.getName().equals(name) && role.equals(db.getRole())).findAny().get();
    }
    catch (NoSuchElementException | HubWebClientException e) {
      //The requested database was not found
      throw new RuntimeException("No database was found with name " + name + " and role " + role, e);
    }
  }

  private static Future<Void> initializeDatabases() {
    return HubWebClientAsync.loadConnectors()
        .compose(connectors -> {
          //TODO: Run the following asynchronously
          for (Connector connector : connectors) {
            loadDatabasesForConnector(connector);
          }
          return Future.succeededFuture();
        });
  }

  private static List<Database> loadDatabasesForConnector(Connector connector) {
    List<Database> databasesFromCache = cache.getIfPresent(connector.id);
    if (databasesFromCache != null)
      return databasesFromCache;

    List<Database> databases = new ArrayList<>();

    if (connector.active) {
      final Map<String, Object> connectorDbSettingsMap = ECPSTool.decryptToMap(CService.configuration.ECPS_PHRASE,
          ConnectorParameters.fromMap(connector.params).getEcps());
      DatabaseSettings connectorDbSettings = new DatabaseSettings(connector.id, connectorDbSettingsMap);

      String rdsClusterId = getClusterIdFromHostname(connectorDbSettings.getHost());

      if (rdsClusterId == null) {
        logger.warn("No cluster ID detected for hostname of DB \"" + connector.id + "\". Taking it into account as simple writer DB.");
        databases.add(new Database(null, null, 128, connectorDbSettingsMap)
            .withName(connector.id)
            .withRole(WRITER));
      }
      else {
        DBCluster dbCluster = AwsRDSClient.getInstance().getRDSClusterConfig(rdsClusterId);
        dbCluster.getDBClusterMembers().forEach(instance -> {
          final DatabaseRole role = instance.isClusterWriter() ? WRITER : READER;

          databases.add(new Database(rdsClusterId, instance.getDBInstanceIdentifier(),
              dbCluster.getServerlessV2ScalingConfiguration().getMaxCapacity(), connectorDbSettingsMap)
              .withName(connector.id)
              .withRole(role));
        });
      }
    }

    cache.put(connector.id, databases);
    return databases;
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
