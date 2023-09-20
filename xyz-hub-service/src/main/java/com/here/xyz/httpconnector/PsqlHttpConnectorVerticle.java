/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.httpconnector.rest.HttpConnectorApi;
import com.here.xyz.httpconnector.rest.HttpMaintenanceApi;
import com.here.xyz.httpconnector.rest.JobApi;
import com.here.xyz.httpconnector.rest.JobStatusApi;
import com.here.xyz.hub.AbstractHttpServerVerticle;
import com.here.xyz.hub.XYZHubRESTVerticle;
import com.here.xyz.psql.PSQLXyzConnector;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;

/**
 * Verticle for HTTP-Connector. Includes three API tribes for :
 * - event Processing
 * - maintenance Tasks
 * - job execution
 */

public class PsqlHttpConnectorVerticle extends AbstractHttpServerVerticle implements AbstractRouterBuilder {

  private static final Logger logger = LogManager.getLogger();
  private static Map<String, String> envMap;
  private AbstractConnectorHandler connector;

  private static String LOCATION = "openapi-http-connector.yaml";
  private static String API;

  static {
    try {
      final byte[] openapi = ByteStreams.toByteArray(XYZHubRESTVerticle.class.getResourceAsStream("/openapi-http-connector.yaml"));
      API = new String(openapi);
    } catch (Exception e) {
      logger.error("Unable to generate OpenApi specs.", e);
    }
  }

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    populateEnvMap();

    if (StringUtils.isEmpty(CService.configuration.ROUTER_BUILDER_CLASS_NAMES)) {
      CService.configuration.ROUTER_BUILDER_CLASS_NAMES = PsqlHttpConnectorVerticle.class.getCanonicalName();
    }
    final List<String> routeBuilderClassList = Arrays.asList(CService.configuration.ROUTER_BUILDER_CLASS_NAMES.split(","));

    final Router mainRouter = Router.router(vertx);
    List<Future> routeFutures = new ArrayList<>();

    for(String routeBuilderClass : routeBuilderClassList) {
      Promise<Void> routePromise = Promise.promise();
      routeFutures.add(routePromise.future());

      try {
        Class<?> cls = Class.forName(routeBuilderClass);
        AbstractRouterBuilder rb = (AbstractRouterBuilder) cls.newInstance();
        Future<Router> routerFuture = rb.buildRoutes(vertx);

        routerFuture.onSuccess(router -> {
          mainRouter.mountSubRouter("/", router);
          routePromise.complete();
        }).onFailure(err -> routePromise.fail(err));

      } catch (Exception e) {
        logger.error("Unable to build routes for {}", routeBuilderClass);
        routePromise.fail(e);
      }
    }

    CompositeFuture.all(routeFutures).onSuccess(done -> {
      addDefaultHandlers(mainRouter);
      createHttpServer(CService.configuration.HTTP_PORT, mainRouter)
              .onSuccess(none -> startPromise.complete())
              .onFailure(err -> startPromise.fail(err));
    }).onFailure(err -> startPromise.fail(err));

  }

  @Override
  public Future<Router> buildRoutes(Vertx vertx) {
    return RouterBuilder.create(vertx, LOCATION).compose(ar -> {
      Router router;
      try {
        final RouterBuilder rb = ar;

        new HttpConnectorApi(rb, new PSQLXyzConnector());
        new HttpMaintenanceApi(rb);
        new JobApi(rb);

        router = rb.createRouter();

        new JobStatusApi(router);

        //OpenAPI resources
        router.route("/psql/static/openapi/*").handler(createCorsHandler()).handler((routingContext -> {
          final HttpServerResponse res = routingContext.response();
          res.putHeader("content-type", "application/yaml");
          final String path = routingContext.request().path();
          if (path.endsWith("openapi-http-connector.yaml")) {
            res.headers().add(CONTENT_LENGTH, String.valueOf(API.getBytes().length));
            res.write(API);
          }
          else {
            res.setStatusCode(HttpResponseStatus.NOT_FOUND.code());
          }
          res.end();
        }));
        router.route().order(1).handler(createCorsHandler());
      }
      catch (Exception e) {
        logger.error("An error occurred, during the creation of the router from the Open API specification file.", e);
        return Future.failedFuture(e);
      }
      return Future.succeededFuture(router);
    });
  }

  public static Map<String, String> getEnvMap() {
    if (envMap != null)
      return envMap;

    populateEnvMap();

    return envMap;
  }

  public static synchronized void populateEnvMap(){
    try {
      envMap = new ObjectMapper().convertValue(CService.configuration, HashMap.class);
    }catch (Exception e){
      logger.error("Cannot populate EnvMap");
    }
  }

}