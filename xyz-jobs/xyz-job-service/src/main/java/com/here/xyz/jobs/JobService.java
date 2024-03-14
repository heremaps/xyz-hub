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

package com.here.xyz.jobs;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.util.service.Core;
import com.here.xyz.util.web.HubWebClientAsync;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class JobService extends Core {
  private static final Logger logger = LogManager.getLogger();
  /**
   * The client to access the Hub REST API
   */
  public static HubWebClientAsync hubWebClient;
  /**
   * Service Configuration
   */
  public static Config configuration;

  public static void main(String[] args) {
    VertxOptions vertxOptions = new VertxOptions()
            .setWorkerPoolSize(NumberUtils.toInt(System.getenv(Core.VERTX_WORKER_POOL_SIZE), 128))
            .setPreferNativeTransport(true)
            .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(15));

    initializeVertx(vertxOptions)
            .compose(Core::initializeConfig)
            .compose(Core::initializeLogger)
            .compose(JobService::parseConfiguration)
            .compose(JobService::initializeClients)
            .compose(JobService::initializeService)
            .onFailure(t -> logger.error("CService startup failed", t))
            .onSuccess(v -> logger.info("Service startup succeeded"));
  }

  private static Future<JsonObject> parseConfiguration(JsonObject config) {
    configuration = config.mapTo(Config.class);
    return Future.succeededFuture(config);
  }

  private static Future<JsonObject> initializeClients(JsonObject config) {
    hubWebClient = HubWebClientAsync.getInstance(configuration.HUB_ENDPOINT);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("HTTP Service is going down at " + new Date());
      // Add operation for shutdown hook
    }));
    return Future.succeededFuture().map(config);
  }
  private static Future<JsonObject> initializeService(JsonObject config) {
    final DeploymentOptions options = new DeploymentOptions()
            .setConfig(config)
            .setWorker(false)
            .setInstances(Runtime.getRuntime().availableProcessors() * 2);

    return vertx.deployVerticle(JobRESTVerticle.class, options)
            .onFailure(t -> logger.error("Unable to deploy JobVerticle.", t))
            .onSuccess(s -> logger.info("Service is up and running on port " + configuration.HTTP_PORT))
            .map(config);
  }

  static {
    XyzSerializable.registerSubtypes(StepGraph.class);
  }
}
