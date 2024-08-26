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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.io.ByteStreams;
import com.here.xyz.util.openapi.OpenApiGenerator;
import com.here.xyz.util.service.AbstractRouterBuilder;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.openapi.router.OpenAPIRoute;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.contract.OpenAPIContract;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class JobRouter implements AbstractRouterBuilder {

  private static final Logger logger = LogManager.getLogger();
  private static String CONTRACT_API;

  static {
    try {
      OpenApiGenerator.getValuesMap().put("SERVER_URL", Config.instance.HUB_ENDPOINT);
      final byte[] openapi = ByteStreams.toByteArray(JobRESTVerticle.class.getResourceAsStream("/openapi.yaml"));
      final byte[] recipes = ByteStreams.toByteArray(JobRESTVerticle.class.getResourceAsStream("/openapi-recipes.yaml"));
      CONTRACT_API = new String(OpenApiGenerator.generate(openapi, recipes, "contract"));
    }
    catch (Exception e) {
      logger.error("Unable to generate OpenApi specs.", e);
    }
  }

  @Override
  public Future<Router> buildRoutes(Vertx vertx) {
    ObjectMapper om = new ObjectMapper(new YAMLFactory());

    try {
      return OpenAPIContract.from(vertx, new JsonObject(om.readValue(CONTRACT_API, Map.class))).compose(contract -> {
        RouterBuilder rb = RouterBuilder.create(vertx, contract);

        for (OpenAPIRoute route : rb.getRoutes())
          route.addHandler(BodyHandler.create());

        initializeJobApi(rb);

        final Router router = rb.createRouter();
        router.route(HttpMethod.GET, "/health").handler(ctx -> ctx.response().send("OK"));

        new JobAdminApi(router);

        return Future.succeededFuture(router);
      });
    }
    catch (JsonProcessingException e) {
      return Future.failedFuture(e);
    }
  }

  private void initializeJobApi(RouterBuilder rb) {
    new JobApi(rb);
  }
}
