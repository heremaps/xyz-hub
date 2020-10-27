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

import static com.here.xyz.hub.rest.Api.CLIENT_CLOSED_REQUEST;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_INFO;
import static com.here.xyz.hub.task.Task.TASK;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static io.vertx.core.http.HttpHeaders.CACHE_CONTROL;
import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static io.vertx.core.http.HttpHeaders.ETAG;
import static io.vertx.core.http.HttpHeaders.IF_MODIFIED_SINCE;
import static io.vertx.core.http.HttpHeaders.IF_NONE_MATCH;
import static io.vertx.core.http.HttpHeaders.USER_AGENT;
import static io.vertx.core.http.HttpMethod.DELETE;
import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.OPTIONS;
import static io.vertx.core.http.HttpMethod.PATCH;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;

import com.here.xyz.hub.auth.Authorization.AuthorizationType;
import com.here.xyz.hub.auth.JWTURIHandler;
import com.here.xyz.hub.auth.JwtDummyHandler;
import com.here.xyz.hub.auth.XyzAuthProvider;
import com.here.xyz.hub.rest.AdminApi;
import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.FeatureApi;
import com.here.xyz.hub.rest.FeatureQueryApi;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.rest.SpaceApi;
import com.here.xyz.hub.rest.health.HealthApi;
import com.here.xyz.hub.task.Task;
import com.here.xyz.hub.util.OpenApiTransformer;
import com.here.xyz.hub.util.logging.LogUtil;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.contract.RouterFactoryOptions;
import io.vertx.ext.web.api.contract.openapi3.OpenAPI3RouterFactory;
import io.vertx.ext.web.handler.AuthHandler;
import io.vertx.ext.web.handler.ChainAuthHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import io.vertx.ext.web.handler.StaticHandler;
import java.io.File;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class XYZHubRESTVerticle extends AbstractVerticle {

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

  /**
   * The methods the client is allowed to use.
   */
  private final List<HttpMethod> allowMethods = Arrays.asList(OPTIONS, GET, POST, PUT, DELETE, PATCH);

  /**
   * The headers, which can be exposed as part of the response.
   */
  private final List<CharSequence> exposeHeaders = Arrays.asList(STREAM_ID, STREAM_INFO, ETAG);

  /**
   * The headers the client is allowed to send.
   */
  private final List<CharSequence> allowHeaders = Arrays.asList(
      AUTHORIZATION, CONTENT_TYPE, USER_AGENT, IF_MODIFIED_SINCE, IF_NONE_MATCH, CACHE_CONTROL, STREAM_ID
  );

  private FeatureApi featureApi;
  private FeatureQueryApi featureQueryApi;
  private SpaceApi spaceApi;
  private HealthApi healthApi;
  private AdminApi adminApi;

  /**
   * The final response handler.
   */
  private static void onResponseEnd(RoutingContext context) {
    final Marker marker = Api.Context.getMarker(context);
    if (!context.response().headWritten()) {
      //The response was closed (e.g. by the client) before it could be written
      logger.info(marker, "The request was cancelled. No response has been sent.");
      onRequestCancelled(context);
    }
    logger.info(marker, "{}", LogUtil.responseToLogEntry(context));
    LogUtil.addResponseInfo(context).end();
    LogUtil.writeAccessLog(context);
  }

  private static void onRequestCancelled(RoutingContext context) {
    context.response().setStatusCode(CLIENT_CLOSED_REQUEST.code());
    context.response().setStatusMessage(CLIENT_CLOSED_REQUEST.reasonPhrase());
    Task task = context.get(TASK);
    if (task != null) {
      //Cancel all pending actions of the task which might be in progress
      task.cancel();
    }
  }

  private static void failureHandler(RoutingContext context) {
    if (context.failure() != null) {
      sendErrorResponse(context, context.failure());
    } else {
      String message = context.statusCode() == 401 ? "Missing auth credentials." : "A failure occurred during the execution.";
      HttpResponseStatus status = context.statusCode() >= 400 ? HttpResponseStatus.valueOf(context.statusCode()) : INTERNAL_SERVER_ERROR;
      sendErrorResponse(context, new HttpException(status, message));
    }
  }

  /**
   * The default NOT FOUND handler.
   */
  private static void notFoundHandler(final RoutingContext context) {
    sendErrorResponse(context, new HttpException(NOT_FOUND, "The requested resource does not exist."));
  }

  /**
   * Creates and sends an error response to the client.
   */
  public static void sendErrorResponse(final RoutingContext context, final Throwable exception) {
    ErrorMessage error;

    try {
      final Marker marker = Api.Context.getMarker(context);

      error = new ErrorMessage(context, exception);
      if (error.statusCode == 500) {
        error.message = null;
        logger.error(marker, "Sending error response: {} {} {}", error.statusCode, error.reasonPhrase, exception);
        logger.error(marker, "Error:", exception);
      }
      else {
        logger.warn(marker, "Sending error response: {} {} {}", error.statusCode, error.reasonPhrase, exception);
        logger.warn(marker, "Error:", exception);
      }
    }
    catch (Exception e) {
      logger.error("Error {} while preparing error response {}", e, exception);
      logger.error("Error:", e);
      logger.error("Original error:", exception);
      error = new ErrorMessage();
    }

    context.response()
        .putHeader(CONTENT_TYPE, APPLICATION_JSON)
        .setStatusCode(error.statusCode)
        .setStatusMessage(error.reasonPhrase)
        .end(Json.encode(error));
  }

  /**
   * The initial request handler.
   */
  private void onRequestReceived(final RoutingContext context) {
    if (context.request().getHeader(STREAM_ID) == null) {
      context.request().headers().add(STREAM_ID, RandomStringUtils.randomAlphanumeric(10));
    }

    //Log the request information.
    LogUtil.addRequestInfo(context);
    context.response().putHeader(STREAM_ID, context.request().getHeader(STREAM_ID));
    context.response().endHandler(ar -> XYZHubRESTVerticle.onResponseEnd(context));
    context.next();
  }

  @Override
  public void start(Future<Void> fut) {
    OpenAPI3RouterFactory.create(vertx, CONTRACT_LOCATION, ar -> {
      if (ar.succeeded()) {
        //Add the handlers
        final OpenAPI3RouterFactory routerFactory = ar.result();
        routerFactory.setOptions(new RouterFactoryOptions());
        featureApi = new FeatureApi(routerFactory);
        featureQueryApi = new FeatureQueryApi(routerFactory);
        spaceApi = new SpaceApi(routerFactory);

        final AuthHandler jwtHandler = createJWTHandler();
        routerFactory.addSecurityHandler("authToken", jwtHandler);

        final Router router = routerFactory.getRouter();
        //Add additional handler to the router
        router.route().failureHandler(XYZHubRESTVerticle::failureHandler);
        router.route().order(0)
            .handler(this::onRequestReceived)
            .handler(createCorsHandler());

        this.healthApi = new HealthApi(vertx, router);
        this.adminApi = new AdminApi(vertx, router, jwtHandler);

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

        //Default NotFound handler
        router.route().last().handler(XYZHubRESTVerticle::notFoundHandler);

        vertx.sharedData().<String, Hashtable<String, Object>>getAsyncMap(Service.SHARED_DATA, sharedDataResult -> {
          sharedDataResult.result().get(Service.SHARED_DATA, hashtableResult -> {
            final Hashtable sharedData = hashtableResult.result();
            final Router globalRouter = (Router) sharedData.get(Service.GLOBAL_ROUTER);

            globalRouter.mountSubRouter("/", router);

            vertx.eventBus().localConsumer(Service.SHARED_DATA, event -> {
              Service.createDefaultHttpServer(globalRouter);
              Service.createAdminHttpServer(globalRouter);
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
   * Add support for cross origin requests.
   */
  private CorsHandler createCorsHandler() {
    CorsHandler cors = CorsHandler.create(".*").allowCredentials(true);
    allowMethods.forEach(cors::allowedMethod);
    allowHeaders.stream().map(String::valueOf).forEach(cors::allowedHeader);
    exposeHeaders.stream().map(String::valueOf).forEach(cors::exposedHeader);
    return cors;
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

  /**
   * Represents an error object response.
   */
  private static class ErrorMessage {

    public String type = "error";
    public int statusCode = INTERNAL_SERVER_ERROR.code();
    public String reasonPhrase = INTERNAL_SERVER_ERROR.reasonPhrase();
    public String message;
    public String streamId;

    public ErrorMessage() {}

    public ErrorMessage(RoutingContext context, Throwable e) {
      Marker marker = Api.Context.getMarker(context);
      this.streamId = marker.getName();
      this.message = e.getMessage();
      if (e instanceof HttpException) {
        this.statusCode = ((HttpException) e).status.code();
        this.reasonPhrase = ((HttpException) e).status.reasonPhrase();
      }

      // The authentication providers do not pass the exception message
      if (statusCode == 401 && message == null) {
        message = "Access to this resource requires valid authentication credentials.";
      }
    }
  }
}
