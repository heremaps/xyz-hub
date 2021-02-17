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

import static com.here.xyz.hub.task.Task.TASK;
import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;

import com.here.xyz.hub.auth.Authorization.AuthorizationType;
import com.here.xyz.hub.auth.JWTURIHandler;
import com.here.xyz.hub.auth.JwtDummyHandler;
import com.here.xyz.hub.auth.XyzAuthProvider;
import com.here.xyz.hub.rest.AdminApi;
import com.here.xyz.hub.rest.ConnectorApi;
import com.here.xyz.hub.rest.FeatureApi;
import com.here.xyz.hub.rest.FeatureQueryApi;
import com.here.xyz.hub.rest.HistoryQueryApi;
import com.here.xyz.hub.rest.SpaceApi;
import com.here.xyz.hub.rest.health.HealthApi;
import com.here.xyz.hub.task.Task;
import com.here.xyz.hub.util.OpenApiTransformer;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.RouterFactoryOptions;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.handler.AuthHandler;
import io.vertx.ext.web.handler.ChainAuthHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.StaticHandler;
import java.io.File;
import java.util.Hashtable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class XYZHubRESTVerticle extends AbstractHttpServerVerticle {

  private static final Logger logger = LogManager.getLogger();

  private static String FULL_API;
  private static String STABLE_API;
  private static String EXPERIMENTAL_API;
  private static String CONTRACT_API;
  private static String CONTRACT_LOCATION;

  static {
    try {
      final OpenApiTransformer openApi = OpenApiTransformer.generateAll();
      FULL_API = openApi.fullApi;
      STABLE_API = openApi.stableApi;
      EXPERIMENTAL_API = openApi.experimentalApi;
      CONTRACT_API = openApi.contractApi;
      CONTRACT_LOCATION = openApi.contractLocation;
    } catch (Exception e) {
      logger.error("Unable to generate OpenApi specs.", e);
    }
  }

  @Override
  protected void onRequestCancelled(RoutingContext context) {
    super.onRequestCancelled(context);
    Task task = context.get(TASK);
    if (task != null) {
      //Cancel all pending actions of the task which might be in progress
      task.cancel();
    }
  }

  @Override
  public void start(Future<Void> fut) {
    OpenAPI3RouterFactory.create(vertx, CONTRACT_LOCATION, ar -> {
      if (ar.succeeded()) {
        //Add the handlers
        final OpenAPI3RouterFactory routerFactory = ar.result();
        routerFactory.setOptions(new RouterFactoryOptions());
        new FeatureApi(routerFactory);
        new FeatureQueryApi(routerFactory);
        new SpaceApi(routerFactory);
        new HistoryQueryApi(routerFactory);
        new ConnectorApi(routerFactory);

        final AuthHandler jwtHandler = createJWTHandler();
        routerFactory.addSecurityHandler("authToken", jwtHandler);

        final Router router = routerFactory.getRouter();

        new HealthApi(vertx, router);
        new AdminApi(vertx, router, jwtHandler);

        //OpenAPI resources
        router.route("/hub/static/openapi/*").handler(createCorsHandler()).handler((routingContext -> {
          final HttpServerResponse res = routingContext.response();
          res.putHeader("content-type", "application/yaml");
          final String path = routingContext.request().path();
          if (path.endsWith("full.yaml")) {
            res.headers().add(CONTENT_LENGTH, String.valueOf(FULL_API.getBytes().length));
            res.write(FULL_API);
          } else if (path.endsWith("stable.yaml")) {
            res.headers().add(CONTENT_LENGTH, String.valueOf(STABLE_API.getBytes().length));
            res.write(STABLE_API);
          } else if (path.endsWith("experimental.yaml")) {
            res.headers().add(CONTENT_LENGTH, String.valueOf(EXPERIMENTAL_API.getBytes().length));
            res.write(EXPERIMENTAL_API);
          } else if (path.endsWith("contract.yaml")) {
            res.headers().add(CONTENT_LENGTH, String.valueOf(CONTRACT_API.getBytes().length));
            res.write(CONTRACT_API);
          } else {
            res.setStatusCode(HttpResponseStatus.NOT_FOUND.code());
          }

          res.end();
        }));

        //Static resources
        router.route("/hub/static/*").handler(StaticHandler.create().setIndexPage("index.html")).handler(createCorsHandler());
        if (Service.configuration.FS_WEB_ROOT != null) {
          logger.debug("Serving extra web-root folder in file-system with location: {}", Service.configuration.FS_WEB_ROOT);
          //noinspection ResultOfMethodCallIgnored
          new File(Service.configuration.FS_WEB_ROOT).mkdirs();
          router.route("/hub/static/*")
              .handler(StaticHandler.create(Service.configuration.FS_WEB_ROOT).setIndexPage("index.html"));
        }

        //Add default handlers
        addDefaultHandlers(router);

        vertx.sharedData().<String, Hashtable<String, Object>>getAsyncMap(Service.SHARED_DATA, sharedDataResult -> {
          sharedDataResult.result().get(Service.SHARED_DATA, hashtableResult -> {
            final Hashtable<String, Object> sharedData = hashtableResult.result();
            final Router globalRouter = (Router) sharedData.get(Service.GLOBAL_ROUTER);

            globalRouter.mountSubRouter("/", router);

            vertx.eventBus().localConsumer(Service.SHARED_DATA, event -> {
              createHttpServer(Service.configuration.HTTP_PORT, globalRouter);
              if (Service.configuration.HTTP_PORT != Service.configuration.ADMIN_MESSAGE_PORT) {
                createHttpServer(Service.configuration.ADMIN_MESSAGE_PORT, globalRouter);
              }
            });

            fut.complete();
          });
        });
      } else {
        logger.error("An error occurred, during the creation of the router from the Open API specification file.", ar.cause());
      }
    });
  }

  /**
   * Add the security handlers.
   */
  private AuthHandler createJWTHandler() {
    JWTAuthOptions authConfig = new JWTAuthOptions().addPubSecKey(
        new PubSecKeyOptions().setAlgorithm("RS256")
            .setPublicKey(Service.configuration.JWT_PUB_KEY));

    JWTAuth authProvider = new XyzAuthProvider(vertx, authConfig);

    ChainAuthHandler authHandler = ChainAuthHandler.create()
        .append(JWTAuthHandler.create(authProvider))
        .append(JWTURIHandler.create(authProvider));

    if (Service.configuration.XYZ_HUB_AUTH == AuthorizationType.DUMMY) {
      authHandler.append(JwtDummyHandler.create(authProvider));
    }

    return authHandler;
  }
}
