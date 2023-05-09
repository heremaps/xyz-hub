/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

import static com.here.xyz.XyzLogger.currentLogger;
import static com.here.xyz.hub.rest.Api.CLIENT_CLOSED_REQUEST;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_INFO;
import static com.here.xyz.hub.rest.Api.HeaderValues.STRICT_TRANSPORT_SECURITY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static io.vertx.core.http.HttpHeaders.CACHE_CONTROL;
import static io.vertx.core.http.HttpHeaders.CONTENT_LENGTH;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static io.vertx.core.http.HttpHeaders.ETAG;
import static io.vertx.core.http.HttpHeaders.IF_MODIFIED_SINCE;
import static io.vertx.core.http.HttpHeaders.IF_NONE_MATCH;
import static io.vertx.core.http.HttpHeaders.LOCATION;
import static io.vertx.core.http.HttpHeaders.USER_AGENT;
import static io.vertx.core.http.HttpMethod.DELETE;
import static io.vertx.core.http.HttpMethod.GET;
import static io.vertx.core.http.HttpMethod.OPTIONS;
import static io.vertx.core.http.HttpMethod.PATCH;
import static io.vertx.core.http.HttpMethod.POST;
import static io.vertx.core.http.HttpMethod.PUT;

import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.here.xyz.hub.auth.NakshaJwtAuthHandler;
import com.here.xyz.hub.auth.NakshaAuthProvider;
import com.here.xyz.hub.rest.ConnectorApi;
import com.here.xyz.hub.rest.FeatureApi;
import com.here.xyz.hub.rest.FeatureQueryApi;
import com.here.xyz.hub.rest.HistoryQueryApi;
import com.here.xyz.hub.rest.SpaceApi;
import com.here.xyz.hub.rest.SubscriptionApi;
import com.here.xyz.hub.rest.health.HealthApi;
import com.here.xyz.hub.task.NakshaTask;
import com.here.xyz.hub.task.feature.AbstractFeatureTask;
import com.here.xyz.hub.util.logging.LogUtil;
import com.here.xyz.util.IoHelp;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.openapi.RouterBuilderOptions;
import io.vertx.ext.web.validation.BadRequestException;
import io.vertx.ext.web.validation.BodyProcessorException;
import io.vertx.ext.web.validation.ParameterProcessorException;
import io.vertx.ext.web.validation.impl.ParameterLocation;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.checkerframework.checker.units.qual.C;
import org.jetbrains.annotations.NotNull;

/**
 * The Naksha-Hub verticle. Can only be created by the Naksha-Hub.
 */
public class NakshaHubVerticle extends AbstractVerticle {

  NakshaHubVerticle(@NotNull NakshaHub hub, int index) {
    this.hub = hub;
    this.index = index;
  }

  /**
   * The Naksha-Hub to which the verticle blongs.
   */
  protected final @NotNull NakshaHub hub;

  /**
   * The index in the {@link NakshaHub#verticles} array.
   */
  protected final int index;

  @Override
  public void start(final @NotNull Promise<Void> startPromise) throws Exception {
    // In a nutshell:
    // When several HTTP servers listen on the same port, vert.x orchestrates the request handling using a round-robin strategy.
    //
    // https://vertx.io/docs/vertx-core/java/#_server_sharing
    RouterBuilder.create(vertx, "openapi.yaml").onComplete(ar -> {
      try {
        if (ar.failed()) {
          throw ar.cause();
        }

        final RouterBuilder rb = ar.result();
        rb.setOptions(new RouterBuilderOptions()
            .setContractEndpoint(RouterBuilderOptions.STANDARD_CONTRACT_ENDPOINT)
            .setRequireSecurityHandlers(false));
        new FeatureApi(rb);
        new FeatureQueryApi(rb);
        new SpaceApi(rb);
        new HistoryQueryApi(rb);
        new ConnectorApi(rb);
        new SubscriptionApi(rb);

        final AuthenticationHandler jwtHandler = new NakshaJwtAuthHandler(new NakshaAuthProvider(vertx, hub.authOptions), null);
        rb.securityHandler("Bearer", jwtHandler);

        final Router router = rb.createRouter();
        new HealthApi(vertx, router);

        // OpenAPI resources
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

        // https://github.com/vert-x3/vertx-web/issues/2182
        System.setProperty("io.vertx.web.router.setup.lenient", "true");

        //Static resources
        final CorsHandler corsHandler = createCorsHandler();
        final Route staticRoute = router.route("/hub/static/*");
        staticRoute.handler(this::serveFromResources).handler(
                new DelegatingHandler<>(StaticHandler.create().setIndexPage("index.html"), context -> context.addHeadersEndHandler(v -> {
                  //This handler implements a workaround for an issue with CloudFront, which removes slashes at the end of the request-URL's path
                  MultiMap headers = context.response().headers();
                  if (headers.contains(LOCATION)) {
                    String headerValue = headers.get(LOCATION);
                    if (headerValue.endsWith("/")) {
                      headers.set(LOCATION, headerValue + "index.html");
                    }
                  }
                }), null))
            .handler(corsHandler);
        if (Service.configuration.FS_WEB_ROOT != null) {
          logger.debug("Serving extra web-root folder in file-system with location: {}", Service.configuration.FS_WEB_ROOT);
          //noinspection ResultOfMethodCallIgnored
          new File(Service.configuration.FS_WEB_ROOT).mkdirs();
          router.route("/hub/static/*")
              .handler(StaticHandler.create()
                  .setAllowRootFileSystemAccess(true)
                  .setWebRoot(Service.configuration.FS_WEB_ROOT)
                  .setIndexPage("index.html")
              );
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

            startPromise.complete();
          });
        });
      } catch (Throwable t) {
        currentLogger().error("An error occurred during the creation of the router from the Open API specification file.", t);
        startPromise.fail(t);
      }
    });
  }

  private static final ConcurrentHashMap<@NotNull String, @NotNull Buffer> fileCache = new ConcurrentHashMap<>();

  /**
   * Handler to server static files.
   */
  protected void serveFromResources(@NotNull RoutingContext routingContext) {
    final String path = routingContext.request().path();
    Buffer buffer = fileCache.get(path);
    // TODO: Do we want to server more files from resources?
    if (buffer == null) {
      if ("/hub/static/openapi.yaml".equals(path)) {
        try {
          buffer = Buffer.buffer(IoHelp.readResourceBytes("openapi.yaml"));
          fileCache.put(path, buffer);
        } catch (IOException e) {
          NakshaTask.currentLogger(routingContext).error("Failed to load openapi.yaml file from resources", e);
        }
      }
    }
    if (buffer != null) {
      sendResponse(routingContext, buffer);
    } else {
      routingContext.next();
    }
  }

  protected void sendResponse(@NotNull RoutingContext routingContext, @NotNull Buffer buffer) {
    final HttpServerResponse response = routingContext.response();
    response.headers().add(CONTENT_LENGTH, String.valueOf(buffer.length()));
    response.write(buffer);
    response.end();
  }

  private static String FULL_API;
  private static String STABLE_API;
  private static String EXPERIMENTAL_API;
  private static String CONTRACT_API;
  private static String CONTRACT_LOCATION;

  static {
    try {
      final byte[] openapi = ByteStreams.toByteArray(Objects.requireNonNull(NakshaHubVerticle.class.getResourceAsStream("/openapi.yaml")));
      final byte[] recipes = ByteStreams.toByteArray(
          Objects.requireNonNull(NakshaHubVerticle.class.getResourceAsStream("/openapi-recipes.yaml")));

      FULL_API = new String(openapi);
      STABLE_API = new String(generate(openapi, recipes, "stable"));
      EXPERIMENTAL_API = new String(generate(openapi, recipes, "experimental"));
      CONTRACT_API = new String(generate(openapi, recipes, "contract"));

      final File tempFile = File.createTempFile("contract-", ".yaml");
      Files.write(CONTRACT_API.getBytes(), tempFile);
      CONTRACT_LOCATION = tempFile.toURI().toString();
    } catch (Exception e) {
      logger.error("Unable to generate OpenApi specs.", e);
    }
  }

  @Override
  protected void onRequestCancelled(RoutingContext context) {
    super.onRequestCancelled(context);
    final NakshaTask<?, ?> task = Context.task(context);
    if (task != null) {
      //Cancel all pending actions of the task which might be in progress
      task.cancel();
    }
  }

  private static void routerFailure(Throwable t) {
    logger.error("An error occurred, during the creation of the router from the Open API specification file.", t);
  }

  /**
   * Add the security handlers.
   */
  private AuthenticationHandler createJWTHandler() {
    String pubKey;
    try {
      final byte[] bytes = Core.readFileFromHomeOrResource("auth/jwt.pub");
      pubKey = new String(bytes, StandardCharsets.UTF_8);
    } catch (Exception e) {
      logger.error("Failed to load JWT public key from home or resources", e);
      // Fallback
      pubKey = Service.configuration.getJwtPubKey();
    }
    assert pubKey != null;
    final JWTAuthOptions authConfig = new JWTAuthOptions().addPubSecKey(new PubSecKeyOptions().setAlgorithm("RS256").setBuffer(pubKey));
    return new NakshaJwtAuthHandler(new NakshaAuthProvider(vertx, authConfig), null);
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


  public static final HttpServerOptions SERVER_OPTIONS = new HttpServerOptions()
      .setCompressionSupported(true)
      .setDecompressionSupported(true)
      .setHandle100ContinueAutomatically(true)
      .setTcpQuickAck(true)
      .setTcpFastOpen(true)
      .setMaxInitialLineLength(16 * 1024)
      .setIdleTimeout(300);
  public static final String STREAM_INFO_CTX_KEY = "streamInfo";

  private static final Logger logger = LogManager.getLogger();
  /**
   * The methods the client allowed to use.
   */
  private final List<HttpMethod> allowMethods = Arrays.asList(OPTIONS, GET, POST, PUT, DELETE, PATCH);

  /**
   * The headers, which can be exposed as part of the response.
   */
  private final List<CharSequence> exposeHeaders = Arrays.asList(STREAM_ID, STREAM_INFO, ETAG);

  /**
   * The headers the client allowed to send.
   */
  private final List<CharSequence> allowHeaders = Arrays.asList(
      AUTHORIZATION, CONTENT_TYPE, USER_AGENT, IF_MODIFIED_SINCE, IF_NONE_MATCH, CACHE_CONTROL, STREAM_ID
  );

  public Future<Void> createHttpServer(int port, Router router) {
    Promise<Void> promise = Promise.promise();

    vertx.createHttpServer(SERVER_OPTIONS)
        .requestHandler(router)
        .listen(port, result -> {
          if (result.succeeded()) {
            logger.info("HTTP Server started on port {}", port);
            promise.complete();
          } else {
            logger.error("An error occurred, during the initialization of the server.", result.cause());
            promise.fail(result.cause());
          }
        });

    return promise.future();
  }

  /**
   * Add default handlers.
   * <p>
   * Call this method after all other routes are defined.
   *
   * @param router
   */
  protected void addDefaultHandlers(Router router) {
    //Add additional handler to the router
    router.route().failureHandler(createFailureHandler());
    // starts at the 2nd route, since the first one is automatically added from openapi's RouterBuilder.createRouter
    router.route().order(1)
        .handler(createReceiveHandler())
        .handler(createMaxRequestSizeHandler())
        .handler(createCorsHandler());
    //Default NotFound handler
    router.route().last().handler(createNotFoundHandler());
  }

  /**
   * The final response handler.
   */
  protected void onResponseEnd(RoutingContext context) {
    final Marker marker = Context.getMarker(context);
    if (!context.response().headWritten()) {
      //The response was closed (e.g. by the client) before it could be written
      logger.info(marker, "The request was cancelled. No response has been sent.");
      onRequestCancelled(context);
    }
    logger.info(marker, "{}", LogUtil.responseToLogEntry(context));
    LogUtil.addResponseInfo(context).end();
    LogUtil.writeAccessLog(context);
  }

  protected void onRequestCancelled(RoutingContext context) {
    context.response().setStatusCode(CLIENT_CLOSED_REQUEST.code());
    context.response().setStatusMessage(CLIENT_CLOSED_REQUEST.reasonPhrase());
  }

  protected Handler<RoutingContext> createFailureHandler() {
    return context -> {
      String message = "A failure occurred during the execution.";
      if (context.failure() != null) {
        Throwable t = context.failure();
        if (t instanceof io.vertx.ext.web.handler.HttpException) {
          //Transform Vert.x HTTP exception into ours
          HttpResponseStatus status = HttpResponseStatus.valueOf(((io.vertx.ext.web.handler.HttpException) t).getStatusCode());
          if (status == UNAUTHORIZED) {
            message = "Missing auth credentials.";
          }
          t = new HttpException(status, message, t);
        }
        if (t instanceof BodyProcessorException) {
          sendErrorResponse(context, new HttpException(BAD_REQUEST, "Failed to parse body."));
          Buffer bodyBuffer = context.getBody();
          String body = null;
          if (bodyBuffer != null) {
            String bodyString = bodyBuffer.toString();
            body = bodyString.substring(0, Math.min(4096, bodyString.length()));
          }

          logger.warn("Exception processing body: {}. Body was: {}", t.getMessage(), body);
        } else if (t instanceof ParameterProcessorException) {
          ParameterLocation location = ((ParameterProcessorException) t).getLocation();
          String paramName = ((ParameterProcessorException) t).getParameterName();
          sendErrorResponse(context, new HttpException(BAD_REQUEST, "Invalid request input parameter value for "
              + location.name().toLowerCase() + "-parameter \"" + location.lowerCaseIfNeeded(paramName) + "\". Reason: "
              + ((ParameterProcessorException) t).getErrorType()));
        } else if (t instanceof BadRequestException) {
          sendErrorResponse(context, new HttpException(BAD_REQUEST, "Invalid request."));
        } else {
          sendErrorResponse(context, t);
        }
      } else {
        HttpResponseStatus status = context.statusCode() >= 400 ? HttpResponseStatus.valueOf(context.statusCode()) : INTERNAL_SERVER_ERROR;
        sendErrorResponse(context, new HttpException(status, message));
      }
    };
  }

  /**
   * The default NOT FOUND handler.
   */
  protected Handler<RoutingContext> createNotFoundHandler() {
    return context -> sendErrorResponse(context, new HttpException(NOT_FOUND, "The requested resource does not exist."));
  }

  /**
   * The max request size handler.
   */
  protected Handler<RoutingContext> createMaxRequestSizeHandler() {
    return context -> {
      if (Service.configuration != null) {
        long limit = Service.configuration.MAX_UNCOMPRESSED_REQUEST_SIZE;

        String errorMessage = "The request payload is bigger than the maximum allowed.";
        String uploadLimit;
        HttpResponseStatus status = REQUEST_ENTITY_TOO_LARGE;

        if (Service.configuration.UPLOAD_LIMIT_HEADER_NAME != null
            && (uploadLimit = context.request().headers().get(Service.configuration.UPLOAD_LIMIT_HEADER_NAME)) != null) {

          try {
            /** Override limit if we are receiving an UPLOAD_LIMIT_HEADER_NAME value */
            limit = Long.parseLong(uploadLimit);

            /** Add limit to streamInfo response header */
            addStreamInfo(context, "MaxReqSize", limit);
          } catch (NumberFormatException e) {
            sendErrorResponse(context, new HttpException(BAD_REQUEST,
                "Value of header: " + Service.configuration.UPLOAD_LIMIT_HEADER_NAME + " has to be a number."));
            return;
          }

          /** Override http response code if its configured */
          if (Service.configuration.UPLOAD_LIMIT_REACHED_HTTP_CODE > 0) {
            status = HttpResponseStatus.valueOf(Service.configuration.UPLOAD_LIMIT_REACHED_HTTP_CODE);
          }

          /** Override error Message if its configured */
          if (Service.configuration.UPLOAD_LIMIT_REACHED_MESSAGE != null) {
            errorMessage = Service.configuration.UPLOAD_LIMIT_REACHED_MESSAGE;
          }
        }

        if (limit > 0) {
          if (context.getBody() != null && context.getBody().length() > limit) {
            sendErrorResponse(context, new HttpException(status, errorMessage));
            return;
          }
        }
      }
      context.next();
    };
  }

  /**
   * The initial request handler.
   */
  protected Handler<RoutingContext> createReceiveHandler() {
    final long maxAge = TimeUnit.MINUTES.toSeconds(1);
    final String customStreamInfoKey = getCustomStreamInfoKey();

    return context -> {
      if (context.request().getHeader(STREAM_ID) == null) {
        context.request().headers().add(STREAM_ID, RandomStringUtils.randomAlphanumeric(10));
      }

      //Log the request information.
      LogUtil.addRequestInfo(context);
      context.response().putHeader(STREAM_ID, context.request().getHeader(STREAM_ID));
      context.response().putHeader(STRICT_TRANSPORT_SECURITY, "max-age=" + maxAge);
      context.response().endHandler(ar -> onResponseEnd(context));
      context.addHeadersEndHandler(v -> headersEndHandler(context, customStreamInfoKey));
      context.next();
    };
  }

  /**
   * Returns the custom Stream-Info key to be added to the headers together with the original Stream-Info header. When set, the stream info
   * values will be duplicated in two different headers during response.
   *
   * @return CUSTOM_STREAM_INFO_HEADER_NAME or null when not set.
   */
  private static String getCustomStreamInfoKey() {
    if (Service.configuration != null && !Strings.isNullOrEmpty(Service.configuration.CUSTOM_STREAM_INFO_HEADER_NAME)) {
      return Service.configuration.CUSTOM_STREAM_INFO_HEADER_NAME;
    }

    return null;
  }

  protected static void headersEndHandler(RoutingContext context, String customStreamInfoKey) {
    Map<String, Object> streamInfo;
    if (context != null && (streamInfo = context.get(STREAM_INFO_CTX_KEY)) != null) {
      String streamInfoValues = "";
      for (Entry<String, Object> e : streamInfo.entrySet()) {
        streamInfoValues += e.getKey() + "=" + e.getValue() + ";";
      }

      context.response().putHeader(STREAM_INFO, streamInfoValues);
      if (customStreamInfoKey != null) {
        context.response().putHeader(customStreamInfoKey, streamInfoValues);
      }
    }
  }

  /**
   * Creates and sends an error response to the client.
   */
  public static void sendErrorResponse(final RoutingContext context, Throwable exception) {
    //If the request was cancelled, neither a response has to be sent nor the error should be logged.
    if (exception instanceof TaskPipelineCancelled) {
      return;
    }
    if (exception instanceof IllegalStateException && exception.getMessage().startsWith("Request method must be one of")) {
      exception = new HttpException(METHOD_NOT_ALLOWED, exception.getMessage(), exception);
    }

    ErrorMessage error;

    try {
      final Marker marker = Context.getMarker(context);

      error = new ErrorMessage(context, exception);
      if (error.statusCode == 500) {
        error.message = null;
        logger.error(marker, "Sending error response: {} {} {}", error.statusCode, error.reasonPhrase, exception);
        logger.error(marker, "Error:", exception);
      } else {
        logger.warn(marker, "Sending error response: {} {} {}", error.statusCode, error.reasonPhrase, exception);
        logger.warn(marker, "Error:", exception);
      }
    } catch (Exception e) {
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
   * Add support for cross-origin requests.
   *
   * @return A handler for cross-origin requests.
   */
  protected @NotNull CorsHandler createCorsHandler() {
    CorsHandler cors = CorsHandler.create(".*").allowCredentials(true);
    allowMethods.forEach(cors::allowedMethod);
    allowHeaders.stream().map(String::valueOf).forEach(cors::allowedHeader);
    exposeHeaders.stream().map(String::valueOf).forEach(cors::exposedHeader);
    return cors;
  }

  public static <T extends AbstractFeatureTask> void addStreamInfo(RoutingContext context, String streamInfoKey, Object streamInfoValue) {
    if (context.get(STREAM_INFO_CTX_KEY) == null) {
      context.put(STREAM_INFO_CTX_KEY, new HashMap<String, Object>());
    }

    ((Map<String, Object>) context.get(STREAM_INFO_CTX_KEY)).put(streamInfoKey, streamInfoValue);
  }

  /**
   * Represents an error object response.
   */
  protected static class ErrorMessage {

    public String type = "error";
    public int statusCode = INTERNAL_SERVER_ERROR.code();
    public String reasonPhrase = INTERNAL_SERVER_ERROR.reasonPhrase();
    public String message;
    public String streamId;

    public ErrorMessage() {
    }

    public ErrorMessage(RoutingContext context, Throwable e) {
      Marker marker = Context.getMarker(context);
      streamId = marker.getName();
      message = e.getMessage();
      if (e instanceof HttpException) {
        statusCode = ((HttpException) e).status.code();
        reasonPhrase = ((HttpException) e).status.reasonPhrase();
      } else if (e instanceof BadRequestException) {
        statusCode = BAD_REQUEST.code();
        reasonPhrase = BAD_REQUEST.reasonPhrase();
      }

      // The authentication providers do not pass the exception message
      if (statusCode == 401 && message == null) {
        message = "Access to this resource requires valid authentication credentials.";
      }
    }
  }
}
