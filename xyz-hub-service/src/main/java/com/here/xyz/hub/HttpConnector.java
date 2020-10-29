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

package com.here.xyz.hub;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.json.JsonObject;

public class HttpConnector extends Core {

  public static JsonObject configuration;

  public static void main(String[] args) {
    initialize(false, "connector-config.json", HttpConnector::onConfigLoaded );
  }

  private static void onConfigLoaded(JsonObject jsonConfig) {
    configuration = jsonConfig;

    final DeploymentOptions options = new DeploymentOptions()
        .setConfig(jsonConfig)
        .setWorker(false)
        .setInstances(Runtime.getRuntime().availableProcessors() * 2);

    vertx.deployVerticle(PsqlHttpVerticle.class, options,result -> {
      if (result.failed()) {
        System.err.println("Unable to deploy the verticle.");
        System.exit(1);
      }
      System.out.println("The connector is up and running on port " + jsonConfig.getInteger("HTTP_PORT") );
    });
  }
}
