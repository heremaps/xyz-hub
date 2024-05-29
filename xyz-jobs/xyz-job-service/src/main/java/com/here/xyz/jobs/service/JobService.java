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

package com.here.xyz.jobs.service;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.CleanUpExecutor;
import com.here.xyz.jobs.steps.execution.JobExecutor;
import com.here.xyz.jobs.steps.inputs.Input;
import com.here.xyz.jobs.steps.outputs.Output;
import com.here.xyz.util.service.Core;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobService extends Core {

  private static final Logger logger = LogManager.getLogger();

  static {
    CONFIG_FILE = "jobs.config.json";
  }

  static {
    XyzSerializable.registerSubtypes(StepGraph.class);
    XyzSerializable.registerSubtypes(Input.class);
    XyzSerializable.registerSubtypes(Output.class);
  }

  public static void main(String[] args) {
    VertxOptions vertxOptions = new VertxOptions()
        .setWorkerPoolSize(NumberUtils.toInt(System.getenv(Core.VERTX_WORKER_POOL_SIZE), 128))
        .setPreferNativeTransport(true)
        .setBlockedThreadCheckInterval(TimeUnit.MINUTES.toMillis(15));

    initializeVertx(vertxOptions)
        .compose(Core::initializeConfig)
        //.compose(Core::initializeLogger) Not needed as the default config is set in the resources
        .compose(JobService::parseConfiguration)
        .compose(JobService::initializeService)
        .compose(JobService::registerShutdownHook)
        .compose(JobService::initializeClients)
        .compose(JobService::startCleanUpThread)
        .onFailure(t -> logger.error("JobService startup failed", t))
        .onSuccess(v -> logger.info("JobService startup succeeded"))
        .onSuccess(v -> JobExecutor.getInstance().init());
  }

  private static Future<JsonObject> parseConfiguration(JsonObject config) {
    config.mapTo(Config.class);
    return Future.succeededFuture(config);
  }

  private static Future<Void> initializeClients(Void v) {
    JobConfigClient.getInstance().init();
    return Future.succeededFuture();
  }

  private static Future<Void> startCleanUpThread(Void v) {
    CleanUpExecutor.getInstance().start();
    return Future.succeededFuture();
  }

  private static Future<Void> initializeService(JsonObject config) {
    final DeploymentOptions options = new DeploymentOptions()
        .setConfig(config)
        .setWorker(false)
        .setInstances(Runtime.getRuntime().availableProcessors() * 2);

    return vertx.deployVerticle(JobRESTVerticle.class, options)
        .onFailure(t -> logger.error("Unable to deploy JobVerticle.", t))
        .onSuccess(s -> logger.info("JobService is up and running on port " + Config.instance.HTTP_PORT))
        .mapEmpty();
  }

  private static Future<Void> registerShutdownHook(Void v) {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      System.out.println("Job Service is going down at " + new Date());
      //Add operation for shutdown hook
    }));
    return Future.succeededFuture();
  }
}
