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

package com.here.xyz.httpconnector;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.here.xyz.httpconnector.config.*;
import com.here.xyz.httpconnector.util.scheduler.ImportQueue;
import com.here.xyz.hub.Core;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Vertex deployment of HTTP-Connector.
 */
public class CService extends Core {

  /**
   * The client to access databases for maintenanceTasks
   */
  public static MaintenanceClient maintenanceClient;

  /**
   * The client to access job configs
   */
  public static JobConfigClient jobConfigClient;

  /**
   * The client to access job configs
   */
  public static JobS3Client jobS3Client;

  /**
   * The client to access job configs
   */
  public static AwsCWClient jobCWClient;

  /**
   * The client to access the database
   */
  public static JDBCImporter jdbcImporter;

  /**
   * Queue for executed Jobs
   */
  public static ImportQueue importQueue;

  /**
   * Service Configuration
   */
  public static Config configuration;;

  public static final List<String> supportedConnectors = new ArrayList<>();
  public static final HashMap<String, String> rdsLookupDatabaseIdentifier = new HashMap<>();
  public static final HashMap<String, Integer> rdsLookupCapacity = new HashMap<>();

  private static final Logger logger = LogManager.getLogger();

  public static void main(String[] args) {
    VertxOptions vertxOptions = new VertxOptions()
            .setWorkerPoolSize(NumberUtils.toInt(System.getenv(Core.VERTX_WORKER_POOL_SIZE), 128))
            .setPreferNativeTransport(true)
            .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(15));
    initialize(vertxOptions, false, "connector-config.json", CService::onConfigLoaded );
  }

  public static void onConfigLoaded(JsonObject jsonConfig) {
    configuration = jsonConfig.mapTo(Config.class);

    try {
      for (String rdsConfig : CService.configuration.JOB_SUPPORTED_RDS) {
        String[] config = rdsConfig.split(":");
        String cId = config[0];
        supportedConnectors.add(cId);
        rdsLookupDatabaseIdentifier.put(cId, config[1]);
        rdsLookupCapacity.put(cId, Integer.parseInt(config[2]));
      }
    }catch (Exception e){
      //logger.error("Configuration-Error - please check service config!");
      throw new RuntimeException("Configuration-Error - please check service config!",e);
    }

    maintenanceClient = new MaintenanceClient();
    jobConfigClient = JobConfigClient.getInstance();

    jobConfigClient.init(spaceConfigReady -> {
      if(spaceConfigReady.succeeded()) {
        jdbcImporter = new JDBCImporter();
        jobS3Client = new JobS3Client();
        jobCWClient = new AwsCWClient();
        importQueue = new ImportQueue();
        /** Start Job-Scheduler */
        importQueue.commence();
      }else
        logger.error("Cant reach dynamoDB - JOB-API deactivated!");
    });

    final DeploymentOptions options = new DeploymentOptions()
            .setConfig(jsonConfig)
            .setWorker(false)
            .setInstances(Runtime.getRuntime().availableProcessors() * 2);

    vertx.deployVerticle(PsqlHttpConnectorVerticle.class, options, result -> {
      if (result.failed()) {
        logger.error("Unable to deploy the verticle.");
        System.exit(1);
      }
      logger.info("The http-connector is up and running on port " + configuration.HTTP_PORT );
    });
  }

  /**
   * The http-connector configuration.
   */
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Config {
    /**
     * Whether the service should use InstanceProviderCredentialsProfile with cached credential when utilizing AWS clients.
     */
    public boolean USE_AWS_INSTANCE_CREDENTIALS_WITH_REFRESH;
    /**
     * The port of the HTTP server.
     */
    public int HTTP_PORT;
    /**
     * ECPS_PHRASE of Default Connector
     */
    public String ECPS_PHRASE;
    /**
     * Max number of parallel running Maintenance Tasks
     */
    public int MAX_CONCURRENT_MAINTENANCE_TASKS;
    /**
     * Defines the time threshold in which a maintenance should be finished. If its reached a
     * warning gets logged.
     */
    public int MISSING_MAINTENANCE_WARNING_IN_HR;
    /**
     * ARN of DynamoDB Table for JOBs
     */
    public String JOBS_DYNAMODB_TABLE_ARN;
    /**
     * S3 Bucket for imports/exports
     */
    public String JOBS_S3_BUCKET;
    /**
     * S3 Bucket for imports/exports
     */
    public String JOBS_S3_BUCKET_REGION;
    /**
     * Set interval for JobQueue processing
     */
    public int JOB_CHECK_QUEUE_INTERVAL_SECONDS;
    /**
     * Define how many job are allowed to run in parallel
     */
    public int JOB_MAX_RUNNING_JOBS;
    /**
     * List of "connectorId:cloudWatchDBInstanceIdentifier:MaxCapacityUnits"
     */
    public List<String> JOB_SUPPORTED_RDS;
    /**
     * RDS maximum ACUs
     */
    public int JOB_MAX_RDS_CAPACITY;
    /**
     * RDS maximum CPU Load in percentage
     */
    public int JOB_MAX_RDS_CPU_LOAD;
    /**
     * RDS maximum allowed import bytes
     */
    public long JOB_MAX_RDS_INFLIGHT_IMPORT_BYTES;
    /**
     * RDS maximum allowed idx creations in parallel
     */
    public int JOB_MAX_RDS_MAX_RUNNING_IDX_CREATIONS;
    /**
     * RDS maximum allowed imports in parallel
     */
    public int JOB_MAX_RDS_MAX_RUNNING_IMPORTS;

    /** ############## Database related ##################### */
    /**
     * Statement Timeout in Seconds
     */
    public int DB_STATEMENT_TIMEOUT_IN_S;
    /**
     * Initial Connection-Pool Size
     */
    public int DB_INITIAL_POOL_SIZE;
    /**
     * Min size of Connection-Pool
     */
    public int DB_MIN_POOL_SIZE;
    /**
     * Max size of Connection-Pool
     */
    public int DB_MAX_POOL_SIZE;
    /**
     * How many connections should get acquired if the pool runs out of available connections.
     */
    public int DB_ACQUIRE_INCREMENT;
    /**
     * How many times will try to acquire a new Connection from the database before giving up.
     */
    public int DB_ACQUIRE_RETRY_ATTEMPTS;
    /**
     * Max Time to wait for a connection checkout - in Seconds
     */
    public int DB_CHECKOUT_TIMEOUT;
    /**
     * Test on checkout if connection is valid
     */
    public boolean DB_TEST_CONNECTION_ON_CHECKOUT;
  }
}

