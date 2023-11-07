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

import com.here.xyz.httpconnector.config.AwsCWClient;
import com.here.xyz.httpconnector.config.AwsSecretManagerClient;
import com.here.xyz.httpconnector.config.JDBCImporter;
import com.here.xyz.httpconnector.config.JobConfigClient;
import com.here.xyz.httpconnector.config.JobS3Client;
import com.here.xyz.httpconnector.util.scheduler.ExportQueue;
import com.here.xyz.httpconnector.util.scheduler.ImportQueue;
import com.here.xyz.httpconnector.util.scheduler.JobQueue;
import com.here.xyz.hub.Core;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
    CONFIG_FILE = "connector-config.json";

    VertxOptions vertxOptions = new VertxOptions()
            .setWorkerPoolSize(NumberUtils.toInt(System.getenv(Core.VERTX_WORKER_POOL_SIZE), 128))
            .setPreferNativeTransport(true)
            .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(15));

    initializeVertx(vertxOptions)
        .compose(Core::initializeConfig)
        .compose(Core::initializeLogger)
        .compose(CService::parseConfiguration)
        .compose(CService::initializeClients)
        .compose(CService::initializeService)
        .onFailure(t -> logger.error("CService startup failed", t))
        .onSuccess(v -> logger.info("CService startup succeeded"));
  }

  private static Future<JsonObject> parseConfiguration(JsonObject config) {
    configuration = config.mapTo(Config.class);

    try {
      for (String rdsConfig : CService.configuration.JOB_SUPPORTED_RDS) {
        String[] splitConfig = rdsConfig.split(":");
        String cId = splitConfig[0];
        supportedConnectors.add(cId);
        rdsLookupCapacity.put(cId, Integer.parseInt(splitConfig[1]));
      }

    } catch (Exception e) {
      logger.error("Configuration-Error - please check service config!");
      throw new RuntimeException("Configuration-Error - please check service config!",e);
    }

    return Future.succeededFuture(config);
  }

  private static Future<JsonObject> initializeClients(JsonObject config) {
    jobConfigClient = JobConfigClient.getInstance();

    return jobConfigClient.init()
        .compose(v -> {
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

          /** Start Job-Schedulers */
          importQueue.commence();
          exportQueue.commence();

          Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("HTTP Service is going down at " + new Date());
            JobQueue.abortAllJobs();
          }));

          return Future.succeededFuture();
        })
        .map(config)
        .onFailure(t -> logger.error("Cant reach jobAPI backend - JOB-API deactivated!", t));
  }

  public static Future<JsonObject> initializeService(JsonObject config) {
    final DeploymentOptions options = new DeploymentOptions()
            .setConfig(config)
            .setWorker(false)
            .setInstances(Runtime.getRuntime().availableProcessors() * 2);

    return vertx.deployVerticle(PsqlHttpConnectorVerticle.class, options)
        .onFailure(t -> logger.error("Unable to deploy http-connector verticle.", t))
        .onSuccess(s -> logger.info("http-connector is up and running on port " + configuration.HTTP_PORT))
        .map(config);
  }
}