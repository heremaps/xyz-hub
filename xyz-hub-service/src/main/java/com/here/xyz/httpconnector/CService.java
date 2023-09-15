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

package com.here.xyz.httpconnector;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.here.xyz.httpconnector.config.*;
import com.here.xyz.httpconnector.util.scheduler.ExportQueue;
import com.here.xyz.httpconnector.util.scheduler.ImportQueue;
import com.here.xyz.httpconnector.util.scheduler.JobQueue;
import com.here.xyz.hub.Core;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Vertex deployment of HTTP-Connector.
 */
public class CService extends Core {
  public static final String USER_AGENT = "HTTP-Connector/" + BUILD_VERSION;

  /**
   * The host ID.
   */
  public static final String HOST_ID = UUID.randomUUID().toString();

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
   * The client to access secrets from AWS Secret Manager
   */
  public static AwsSecretManagerClient jobSecretClient;

  /**
   * The client to access the database
   */
  public static JDBCImporter jdbcImporter;

  /**
   * Queue for executed importJobs
   */
  public static ImportQueue importQueue;

  /**
   * Queue for executed exportJobs
   */
  public static ExportQueue exportQueue;

  /**
   * Service Configuration
   */
  public static Config configuration;
  /**
   * A web client to access XYZ Hub and other web resources.
   */
  public static WebClient webClient;

  public static final List<String> supportedConnectors = new ArrayList<>();
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
        rdsLookupCapacity.put(cId, Integer.parseInt(config[1]));
      }
      supportedConnectors.add(JDBCClients.CONFIG_CLIENT_ID);
    }catch (Exception e){
      logger.error("Configuration-Error - please check service config!");
      throw new RuntimeException("Configuration-Error - please check service config!",e);
    }

    jobConfigClient = JobConfigClient.getInstance();

    jobConfigClient.init(jobConfigReady -> {
      if(jobConfigReady.succeeded()) {
        /** Init webclient */
        webClient = WebClient.create(vertx, new WebClientOptions()
                .setUserAgent(USER_AGENT)
                .setTcpKeepAlive(true)
                .setIdleTimeout(60)
                .setTcpQuickAck(true)
                .setTcpFastOpen(true));

        jobSecretClient = new AwsSecretManagerClient();
        jobS3Client = new JobS3Client();
        jobCWClient = new AwsCWClient();
        importQueue = new ImportQueue();
        exportQueue = new ExportQueue();

        /** Initial initialization of all clients */
        for (String connectorId : supportedConnectors) {
            JDBCClients.addClientsIfRequired(connectorId,true)
                    .onFailure(e -> logger.error("Could not load JDBC Client for connector {}",connectorId, e));
        }

        /** Start Job-Schedulers */
        importQueue.commence();
        exportQueue.commence();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          logger.warn("HTTP Service is going down at " + new Date());
          JobQueue.abortAllJobs();
        }));

      }else
        logger.error("Cant reach jobAPI backend - JOB-API deactivated!");
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
     * The arn of the secret (in Secret Manager) that contains bot credentials.
     */
    public String JOB_BOT_SECRET_ARN;
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
     * S3/CW/Dynamodb localstack endpoints
     */
    public String LOCALSTACK_ENDPOINT;
    /**
     * S3 Bucket for imports/exports
     */
    public String JOBS_S3_BUCKET;
    /**
     * Region in which components are running/hosted
     */
    public String JOBS_REGION;
    /**
     * Set interval for JobQueue processing
     */
    public int JOB_CHECK_QUEUE_INTERVAL_MILLISECONDS;
    /**
     * List of "connectorId:cloudWatchDBInstanceIdentifier:MaxCapacityUnits"
     */
    public List<String> JOB_SUPPORTED_RDS;
    /**
     * RDS maximum ACU Utilization 0-100
     */
    public int JOB_MAX_RDS_MAX_ACU_UTILIZATION;
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
    public int JOB_MAX_RDS_MAX_RUNNING_IMPORT_QUERIES;
    /**
     * RDS maximum allowed imports in parallel
     */
    public int JOB_MAX_RDS_MAX_RUNNING_EXPORT_QUERIES;
    /**
     * RDS maximum allowed imports in parallel
     */
    public Long JOB_DYNAMO_EXP_IN_DAYS;
    /**
     *  DB Pool size per client
     */
    public Integer JOB_DB_POOL_SIZE_PER_CLIENT;
    /**
     *  DB Pool size per status client
     */
    public Integer JOB_DB_POOL_SIZE_PER_STATUS_CLIENT;
    /**
     *  DB Pool size per maintenance client
     */
    public Integer JOB_DB_POOL_SIZE_PER_MAINTENANCE_CLIENT;
    /** ############## Database related ##################### */
    /**
     * Statement Timeout in Seconds
     */
    public int DB_STATEMENT_TIMEOUT_IN_S;
    /**
     * How many times will try to acquire a new Connection from the database before giving up.
     */
    public int DB_ACQUIRE_RETRY_ATTEMPTS;
    /**
     * Max Time to wait for a connection checkout - in Seconds
     */
    public int DB_CHECKOUT_TIMEOUT;

    /** Store Jobs inside DB - only possible if no JOBS_DYNAMODB_TABLE_ARN is defined */
    /**
     * The PostgreSQL URL.
     */
    public String STORAGE_DB_URL;
    /**
     * The database user.
     */
    public String STORAGE_DB_USER;
    /**
     * The database password.
     */
    public String STORAGE_DB_PASSWORD;
    /**
     * Hub-Endpoint
     */
    public String HUB_ENDPOINT;
  }
}