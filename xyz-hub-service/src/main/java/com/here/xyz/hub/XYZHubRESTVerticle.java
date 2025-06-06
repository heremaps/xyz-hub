/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
import static com.here.xyz.util.openapi.OpenApiGenerator.generate;
import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;
import static io.vertx.core.http.HttpHeaders.LOCATION;

import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.here.xyz.hub.auth.ExtendedJWTAuthHandler;
import com.here.xyz.hub.auth.XyzAuthProvider;
import com.here.xyz.hub.rest.AdminApi;
import com.here.xyz.hub.rest.ChangesetApi;
import com.here.xyz.hub.rest.ConnectorApi;
import com.here.xyz.hub.rest.FeatureApi;
import com.here.xyz.hub.rest.FeatureQueryApi;
import com.here.xyz.hub.rest.JobProxyApi;
import com.here.xyz.hub.rest.SpaceApi;
import com.here.xyz.hub.rest.SubscriptionApi;
import com.here.xyz.hub.rest.TagApi;
import com.here.xyz.hub.rest.health.HealthApi;
import com.here.xyz.hub.task.FeatureTask;
import com.here.xyz.hub.task.Task;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.openapi.router.OpenAPIRoute;
import io.vertx.ext.web.openapi.router.RouterBuilder;
import io.vertx.openapi.contract.OpenAPIContract;
import java.io.File;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Objects;
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
      final byte[] openapi = ByteStreams.toByteArray(Objects.requireNonNull(XYZHubRESTVerticle.class.getResourceAsStream("/openapi.yaml")));
      final byte[] recipes = ByteStreams.toByteArray(
          Objects.requireNonNull(XYZHubRESTVerticle.class.getResourceAsStream("/openapi-recipes.yaml")));

      FULL_API = new String(openapi);
      STABLE_API = new String(generate(openapi, recipes, "stable"));
      EXPERIMENTAL_API = new String(generate(openapi, recipes, "experimental"));
      CONTRACT_API = new String(generate(openapi, recipes, "contract"));

      final File tempFile = File.createTempFile("contract-", ".yaml");
      Files.write(CONTRACT_API.getBytes(), tempFile);
      CONTRACT_LOCATION = tempFile.getCanonicalPath();
    } catch (Exception e) {
      logger.error("Unable to generate OpenApi specs.", e);
    }
  }

  public static <T extends FeatureTask> void addStreamInfo(RoutingContext context, String streamInfoKey, Object streamInfoValue) {
    if (context.get(STREAM_INFO_CTX_KEY) == null)
      context.put(STREAM_INFO_CTX_KEY, new HashMap<String, Object>());

    ((Map<String, Object>) context.get(STREAM_INFO_CTX_KEY)).put(streamInfoKey, streamInfoValue);
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
  public void start(Promise<Void> startPromise) throws Exception {
    OpenAPIContract.from(vertx, CONTRACT_LOCATION)
        .compose(this::buildRoutes)
        .onSuccess(none -> startPromise.complete())
        .onFailure(throwable -> {
          startPromise.fail(throwable);
          routerFailure(throwable);
        });
  }

  private Future<Void> buildRoutes(OpenAPIContract contract) {

    try {
      RouterBuilder rb = RouterBuilder.create(vertx, contract);

      final AuthenticationHandler jwtHandler = createJWTHandler();
      for (OpenAPIRoute route : rb.getRoutes()) {
        route.addHandler(createBodyHandler());
        route.addHandler(jwtHandler);
      }

      new FeatureApi(rb);
      new FeatureQueryApi(rb);
      new SpaceApi(rb);
      new ConnectorApi(rb);
      new SubscriptionApi(rb);
      new ChangesetApi(rb);
      new JobProxyApi(rb);
      new TagApi(rb);

      final Router router = rb.createRouter();

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
      router.route("/hub/static/*")
          .handler(createCorsHandler())
          .handler(
              new DelegatingHandler<>(StaticHandler.create().setIndexPage("index.html"), context -> context.addHeadersEndHandler(v -> {
                //This handler implements a workaround for an issue with CloudFront, which removes slashes at the end of the request-URL's path
                MultiMap headers = context.response().headers();
                if (headers.contains(LOCATION)) {
                  String headerValue = headers.get(LOCATION);
                  if (headerValue.endsWith("/")) {
                    headers.set(LOCATION, headerValue + "index.html");
                  }
                }
              }), null));
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
          //Default NotFound handler
          globalRouter.route().last().handler(createNotFoundHandler());

          vertx.eventBus().localConsumer(Service.SHARED_DATA, event -> {
            //Create the main service listener
            createHttpServer(Service.configuration.HTTP_PORT, globalRouter);

            //Create the main service TLS listener
            if (Service.configuration.XYZ_HUB_HTTPS_PORT > 0
                && Service.configuration.XYZ_HUB_SERVER_TLS_KEY != null && Service.configuration.XYZ_HUB_SERVER_TLS_CERT != null)
              createHttpServerWithMutualTls(Service.configuration.XYZ_HUB_HTTPS_PORT, globalRouter, Service.configuration.XYZ_HUB_SERVER_TLS_KEY,
                  Service.configuration.XYZ_HUB_SERVER_TLS_CERT, Service.configuration.XYZ_HUB_CLIENT_TLS_TRUSTSTORE);

            //Create the admin messaging listener
            if (Service.configuration.HTTP_PORT != Service.configuration.ADMIN_MESSAGE_PORT) {
              createHttpServer(Service.configuration.ADMIN_MESSAGE_PORT, globalRouter);
            }
          });
        });
      });
    } catch (Exception e) {
      return Future.failedFuture(e);
    }

    return Future.succeededFuture();
  }

  private static void routerFailure(Throwable t) {
    logger.error("An error occurred, during the creation of the router from the Open API specification file.", t);
  }

  /**
   * Add the security handlers.
   */
  private AuthenticationHandler createJWTHandler() {
    JWTAuthOptions authConfig = new JWTAuthOptions().addPubSecKey(
        new PubSecKeyOptions().setAlgorithm("RS256")
            .setBuffer(Service.configuration.getJwtPubKey()));

    return new ExtendedJWTAuthHandler(new XyzAuthProvider(vertx, authConfig), null);
  }

  private static class DelegatingHandler<E> implements Handler<E> {

    private final Handler<E> before;
    private final Handler<E> delegate;
    private final Handler<E> after;

    DelegatingHandler(Handler<E> delegate, Handler<E> before, Handler<E> after) {
      assert delegate != null;
      this.before = before;
      this.delegate = delegate;
      this.after = after;
    }

    @Override
    public void handle(E event) {
      if (before != null) {
        before.handle(event);
      }
      delegate.handle(event);
      if (after != null) {
        after.handle(event);
      }
    }
  }
}
