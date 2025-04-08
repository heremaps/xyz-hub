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
package com.here.naksha.app.service.http;

import static com.here.naksha.app.service.http.NakshaHttpHeaders.STREAM_ID;
import static com.here.naksha.app.service.http.NakshaHttpHeaders.STREAM_INFO;
import static com.here.naksha.app.service.http.auth.actions.JwtUtil.*;
import static com.here.naksha.lib.core.exceptions.UncheckedException.cause;
import static com.here.naksha.lib.core.models.XyzError.ILLEGAL_ARGUMENT;
import static com.here.naksha.lib.core.util.MIMEType.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.*;
import static io.vertx.core.http.HttpMethod.*;
import static io.vertx.core.http.HttpMethod.GET;

import com.here.naksha.app.service.AbstractNakshaHubVerticle;
import com.here.naksha.app.service.NakshaApp;
import com.here.naksha.app.service.http.apis.*;
import com.here.naksha.app.service.http.auth.JWTPayload;
import com.here.naksha.app.service.http.auth.NakshaJwtAuthHandler;
import com.here.naksha.app.service.util.logging.AccessLog;
import com.here.naksha.app.service.util.logging.AccessLogUtil;
import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.BinaryResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.NotModifiedResponse;
import com.here.naksha.lib.core.storage.ModifyFeaturesResp;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.MIMEType;
import com.here.naksha.lib.core.util.StreamInfo;
import com.here.naksha.lib.hub.NakshaHubConfig;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.*;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.*;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.openapi.RouterBuilderOptions;
import io.vertx.ext.web.validation.BadRequestException;
import io.vertx.ext.web.validation.BodyProcessorException;
import io.vertx.ext.web.validation.ParameterProcessorException;
import io.vertx.ext.web.validation.impl.ParameterLocation;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Naksha-Hub verticle. Can only be created by the Naksha-Hub. We deploy as many verticle as we have CPUs.
 */
@SuppressWarnings("unused")
public final class NakshaHttpVerticle extends AbstractNakshaHubVerticle {

  private static final Logger log = LoggerFactory.getLogger(NakshaHttpVerticle.class);

  private final NakshaHubConfig hubConfig;

  /**
   * Creates a new verticle. Each verticle will be bound to a single IO worker.
   *
   * @param naksha The Naksha-Hub.
   * @param index  The verticle index.
   */
  public NakshaHttpVerticle(@NotNull INaksha naksha, int index, @NotNull NakshaApp app) {
    super(naksha, index, app);

    corsHandler = CorsHandler.create()
        .addRelativeOrigin(".*")
        .allowedHeader("*")
        .allowCredentials(true)
        .maxAgeSeconds(86400);

    // The methods the client allowed to use.
    final List<HttpMethod> allowMethods = Arrays.asList(OPTIONS, GET, POST, PUT, DELETE, PATCH, HEAD);
    allowMethods.forEach(corsHandler::allowedMethod);

    // The headers, which can be exposed as part of the response.
    final List<CharSequence> exposeHeaders = Arrays.asList(STREAM_ID, STREAM_INFO, ETAG);
    exposeHeaders.stream().map(String::valueOf).forEach(corsHandler::exposedHeader);

    hubConfig = naksha.getConfig();
    if (hubConfig.webRoot != null) {
      staticHandler = StaticHandler.create(FileSystemAccess.ROOT, hubConfig.webRoot)
          .setIndexPage("index.html")
          .setDirectoryListing(true)
          .setIncludeHidden(false);
    } else {
      staticHandler = null;
    }
  }

  @Override
  public void start(final @NotNull Promise<Void> startPromise) {
    RouterBuilder.create(vertx, "swagger/openapi.yaml").onComplete(ar -> {
      try {
        if (ar.failed()) {
          throw ar.cause();
        }

        final RouterBuilder rb = ar.result();
        rb.setOptions(new RouterBuilderOptions().setRequireSecurityHandlers(false));

        // TODO: We need to change this, so that all these handlers are merged into one handler that is added to
        // the route builder.
        //       The route builder has a method rb.rootHandler(handler) for this purpose!
        // Add default handlers to be executed before all other routes.
        //        final Route earlyRoute = router.route();
        //        earlyRoute
        //            .order(-1)
        //            .handler(this::onNewRequest)
        //            .handler(this::maxRequestSizeHandler)
        //            .handler(this.corsHandler)
        //            .handler(BodyHandler.create()
        //                .setBodyLimit(Integer.MAX_VALUE - 65535)
        //                .setHandleFileUploads(false)
        //                .setPreallocateBodyBuffer(true));

        final AuthenticationHandler jwtHandler = new NakshaJwtAuthHandler(app().authProvider, hubConfig, null);
        rb.securityHandler("Bearer", jwtHandler);

        final List<@NotNull Api> apiControllers = List.of(
            new HealthApi(this),
            new StorageApi(this),
            new SpaceApi(this),
            new EventHandlerApi(this),
            new ReadFeatureApi(this),
            new WriteFeatureApi(this));

        // Add automatic routes.
        for (final Api api : apiControllers) {
          api.addOperations(rb);
        }

        // Add manual routes.
        final Router router = rb.createRouter();
        for (final Api api : apiControllers) {
          api.addManualRoutes(router);
        }

        // CORS handler for all routes
        router.route().order(-2).handler(corsHandler);

        // Static resources route.
        router.route("/hub/static/*").handler(this::onResourceRequest);
        // Swagger doc route.
        router.route("/hub/swagger/*").handler(this::onResourceRequest);

        // Optional: Web server.
        if (staticHandler != null) {
          log.atInfo()
              .setMessage("Serving extra web-root folder in file-system with location: {}")
              .addArgument(hubConfig.webRoot)
              .log();
          router.route("/hub/web/*").handler(staticHandler);
        }

        // When a resource not found, return 404.
        router.route().last().handler(this::notFoundHandler);

        // If any error happened that was not handled otherwise.
        router.route().failureHandler(this::failureHandler);

        // add handler to set max allowed request payload size
        log.info(
            "Setting Http request body limit to {} MB and Header limit to {} KB",
            hubConfig.requestBodyLimit,
            hubConfig.requestHeaderLimit);
        router.route()
            .order(-1) // we add this before any other handler
            .handler(BodyHandler.create()
                .setBodyLimit(hubConfig.requestBodyLimit * 1024 * 1024)
                .setHandleFileUploads(false)
                .setPreallocateBodyBuffer(true));

        // starts at the 2nd route, since the first one is automatically added from openapi's
        // RouterBuilder.createRouter
        router.route().order(1).handler(this::onNewRequest);

        // Add the HTTP server.
        // When several HTTP servers listen on the same port, vert.x orchestrates the request handling using a
        // round-robin strategy.
        //
        // https://vertx.io/docs/vertx-core/java/#_server_sharing
        final HttpServerOptions SERVER_OPTIONS = new HttpServerOptions()
            .setCompressionSupported(true)
            .setDecompressionSupported(true)
            .setHandle100ContinueAutomatically(true)
            .setTcpQuickAck(true)
            .setTcpFastOpen(true)
            .setMaxInitialLineLength(16 * 1024 * 1024) // MB to Bytes
            .setMaxHeaderSize(hubConfig.requestHeaderLimit * 1024) // KB to Bytes
            .setIdleTimeout(300);

        vertx.createHttpServer(SERVER_OPTIONS)
            .requestHandler(router)
            .connectionHandler(loggingConnectionHandler())
            .listen(hubConfig.httpPort, result -> {
              if (result.succeeded()) {
                log.atInfo()
                    .setMessage("HTTP Server started on port {}")
                    .addArgument(hubConfig.httpPort)
                    .log();
                startPromise.complete();
              } else {
                log.atError()
                    .setMessage("An error occurred, during the initialization of the server.")
                    .setCause(result.cause())
                    .log();
                startPromise.fail(result.cause());
              }
            });
      } catch (Throwable t) {
        log.atError()
            .setMessage(
                "An error occurred during the creation of the router from the Open API specification file.")
            .setCause(t)
            .log();
        startPromise.fail(t);
      }
    });
  }

  private final @NotNull CorsHandler corsHandler;
  private final @Nullable StaticHandler staticHandler;

  private static final ConcurrentHashMap<@NotNull String, @NotNull Buffer> fileCache = new ConcurrentHashMap<>();

  private static @Nullable String contentType(@NotNull String filename) {
    final int i = filename.lastIndexOf('.'); // "f.jpg"
    if (i < 0 || i + 1 < filename.length()) {
      return null;
    }
    final String ext = filename.substring(i);
    return MIMEType.getByExtension(ext);
  }

  /**
   * Handler to server static files.
   */
  private void onResourceRequest(@NotNull RoutingContext routingContext) {
    final String path = routingContext.request().path();
    final Buffer cachedBuffer = fileCache.get(path);
    if (cachedBuffer != null) {
      sendRawResponse(routingContext, OK, contentType(path), cachedBuffer);
      return;
    }
    if (!path.startsWith("/hub/static/") && !path.startsWith("/hub/swagger/")) {
      routingContext.next();
      return;
    }
    final String relativePath = path.substring("/hub/".length());
    try {
      routingContext.vertx().executeBlocking((promise -> {
        try {
          final Buffer buffer = Buffer.buffer(IoHelp.readResourceBytes(relativePath));
          fileCache.put(path, buffer);
          sendRawResponse(routingContext, OK, contentType(path), buffer);
        } catch (Throwable t) {
          final Throwable cause = cause(t);
          if (cause instanceof FileNotFoundException e) {
            log.atInfo()
                .setMessage("File not found in resources: {}")
                .addArgument(relativePath)
                .setCause(e)
                .log();
            routingContext.next();
          } else {
            log.atError()
                .setMessage("Unexpected error while trying to load file: {}")
                .addArgument(relativePath)
                .setCause(t)
                .log();
            routingContext.next();
          }
        } finally {
          promise.complete();
        }
      }));
    } catch (RejectedExecutionException t) {
      log.atWarn()
          .setMessage("Failed to load resource, no more worker")
          .setCause(t)
          .log();
      sendErrorResponse(
          routingContext,
          new XyzErrorException(XyzError.TOO_MANY_REQUESTS, "Failed to load resource, no more worker"));
    }
  }

  /**
   * Handler to return 404 Not found.
   *
   * @param routingContext The routing context.
   */
  private void notFoundHandler(final @NotNull RoutingContext routingContext) {
    routingContext
        .response()
        .setStatusCode(NOT_FOUND.code())
        .setStatusMessage(NOT_FOUND.reasonPhrase())
        .end();
  }

  /**
   * A handler that will be called just before headers are written to the response.
   *
   * @param routingContext The routing context.
   */
  private void onHeadersEnd(final @NotNull RoutingContext routingContext) {
    final StreamInfo streamInfo = AccessLogUtil.getStreamInfo(routingContext);
    if (streamInfo != null) {
      routingContext.response().putHeader(STREAM_INFO, streamInfo.toColonSeparatedString());
    }
  }

  /**
   * An end handler for the response. This will be called when the response is disposed to allow consistent cleanup of the response.
   */
  private void onResponseEnd(final @NotNull RoutingContext routingContext) {
    if (!routingContext.response().headWritten()) {
      // The response closed e.g. by the client, before it could be written.
      log.info("The request was cancelled. No response has been sent.");
      onRequestCancelled(routingContext);
    }
    final AccessLog accessLog = AccessLogUtil.addResponseInfo(routingContext);
    if (accessLog == null) return;
    accessLog.end();
    AccessLogUtil.writeAccessLog(routingContext);
  }

  private static final HttpResponseStatus CLIENT_CLOSED_REQUEST =
      new HttpResponseStatus(499, "Client closed request");

  private void onRequestCancelled(@NotNull RoutingContext routingContext) {
    final AbstractTask<?, ?> task = AbstractTask.currentTask();
    if (task != null) {
      if (task.cancel()) {
        log.info("Successfully cancelled task for client side closed connection");
      } else {
        log.info("Failed to cancel task for client side closed connection");
      }
    }
    routingContext.response().setStatusCode(CLIENT_CLOSED_REQUEST.code());
    routingContext.response().setStatusMessage(CLIENT_CLOSED_REQUEST.reasonPhrase());
  }

  private void failureHandler(final @NotNull RoutingContext routingContext) {
    assert routingContext.failure() != null;
    if (routingContext.failure() != null) {
      sendErrorResponse(routingContext, routingContext.failure());
    } else {
      routingContext.next();
    }
  }

  /**
   * The max request size handler.
   */
  private void maxRequestSizeHandler(@NotNull RoutingContext routingContext) {
    // TODO: Should we somehow limit the sizes?
    //       We could do this after authentication and then limit for some clients and not for others?
    routingContext.next();
  }

  static final String STRICT_TRANSPORT_SECURITY = "Strict-Transport-Security";

  /**
   * The handler to be called as first handler.
   *
   * @param routingContext The routing context.
   */
  private void onNewRequest(final @NotNull RoutingContext routingContext) {
    // Add request information, first read in onResponseEnd.
    AccessLogUtil.addRequestInfo(routingContext);
    routingContext.response().endHandler(v -> onResponseEnd(routingContext));
    routingContext.addHeadersEndHandler(v -> onHeadersEnd(routingContext));
    routingContext.next();
  }

  /**
   * @return simple http connection handler that logs closing and failing connections
   */
  private Handler<HttpConnection> loggingConnectionHandler() {
    return httpConnection -> httpConnection
        .closeHandler(v -> log.info("Closing connection"))
        .exceptionHandler(t -> log.info("Connection exception", t));
  }

  private static final Pattern FATAL_ERROR_MSG_PATTERN = Pattern.compile("^[0-9a-zA-Z.-_\\-]+$");

  /**
   * Send an error response for the given XyzError.
   *
   * @param routingContext The routing context for which to send the response.
   * @param xyzError       The XyzError indicating the cause of error
   * @param message        the brief message of the error
   * @return xyzResponse object representing error
   */
  public @NotNull XyzResponse sendErrorResponse(
      final @NotNull RoutingContext routingContext,
      final @NotNull XyzError xyzError,
      final @NotNull String message) {
    final ErrorResponse response = new ErrorResponse(xyzError, message, AccessLogUtil.getStreamId(routingContext));
    sendRawResponse(
        routingContext,
        mapErrorToHttpStatus(response.getError()),
        APPLICATION_JSON,
        Buffer.buffer(response.serialize()));
    return response;
  }

  /**
   * Send an error response for the given exception.
   *
   * @param routingContext The routing context for which to send the response.
   * @param throwable      The exception for which to send an error response.
   */
  public void sendErrorResponse(@NotNull RoutingContext routingContext, @NotNull Throwable throwable) {
    final String streamId = AccessLogUtil.getStreamId(routingContext);
    try {
      final ErrorResponse response;
      final HttpResponseStatus httpStatus;
      if (throwable instanceof HttpException e) {
        log.atInfo()
            .setMessage("Uncaught HTTP exception: {}")
            .addArgument(e.getMessage())
            .setCause(e)
            .log();
        response = new ErrorResponse(XyzError.EXCEPTION, e.getMessage(), streamId);
      } else if (throwable instanceof BodyProcessorException e) {
        response = new ErrorResponse(ILLEGAL_ARGUMENT, e.getMessage(), streamId);
        String bodyPart = null;
        {
          final RequestBody body = routingContext.body();
          final Buffer bodyBuffer;
          if (body != null && (bodyBuffer = body.buffer()) != null) {
            try {
              bodyPart = bodyBuffer.getString(0, Math.min(4096, bodyBuffer.length()));
            } catch (Throwable ignore) {
              bodyPart = "binary";
            }
          }
        }
        log.atInfo()
            .setMessage("Failed to process body, reason: {}, body-start: {}")
            .addArgument(e.getMessage())
            .addArgument(bodyPart)
            .setCause(e)
            .log();
      } else if (throwable instanceof ParameterProcessorException e) {
        final ParameterLocation location = e.getLocation();
        final String paramName = location.lowerCaseIfNeeded(e.getParameterName());
        final String locationName = location.lowerCaseIfNeeded(location.name());
        final String msg = "Invalid request input parameter value for " + locationName + "-parameter '"
            + paramName + "'. " + "Reason: " + e.getErrorType();
        response = new ErrorResponse(ILLEGAL_ARGUMENT, msg, streamId);
        log.atInfo().setMessage(msg).setCause(e).log();
      } else if (throwable instanceof BadRequestException e) {
        final String msg = "Bad Request: " + e.getMessage();
        response = new ErrorResponse(ILLEGAL_ARGUMENT, msg, streamId);
        log.atInfo().setMessage(msg).setCause(e).log();
      } else {
        response = new ErrorResponse(throwable, streamId);
        log.atInfo()
            .setMessage("Send error response based upon unhandled exception: {}")
            .addArgument(throwable.getMessage())
            .setCause(throwable)
            .log();
      }
      assert streamId.equals(response.getStreamId());
      assert response.getError() != null;
      assert response.getErrorMessage() != null;
      if (throwable instanceof HttpException e) {
        httpStatus = HttpResponseStatus.valueOf(e.getStatusCode());
      } else {
        httpStatus = mapErrorToHttpStatus(response.getError());
      }
      sendRawResponse(routingContext, httpStatus, APPLICATION_JSON, Buffer.buffer(response.serialize()));
    } catch (Throwable t) {
      log.atError()
          .setMessage("Unexpected error while generating error response")
          .setCause(t)
          .log();
      log.atError()
          .setMessage("Failed to turn an exception into an error message")
          .setCause(throwable)
          .log();
      //noinspection ConstantConditions
      final String msg = throwable != null ? throwable.getMessage() : "sendErrorResponse(ctx,null)";
      sendFatalErrorResponse(routingContext, msg);
    }
  }

  private @NotNull HttpResponseStatus mapErrorToHttpStatus(final @NotNull XyzError xyzError) {
    if (xyzError.equals(XyzError.EXCEPTION)) {
      return HttpResponseStatus.INTERNAL_SERVER_ERROR;
    } else if (xyzError.equals(XyzError.NOT_IMPLEMENTED)) {
      return HttpResponseStatus.NOT_IMPLEMENTED;
    } else if (xyzError.equals(ILLEGAL_ARGUMENT)) {
      return HttpResponseStatus.BAD_REQUEST;
    } else if (xyzError.equals(XyzError.PAYLOAD_TOO_LARGE)) {
      return HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
    } else if (xyzError.equals(XyzError.BAD_GATEWAY)) {
      return HttpResponseStatus.BAD_GATEWAY;
    } else if (xyzError.equals(XyzError.CONFLICT)) {
      return HttpResponseStatus.CONFLICT;
    } else if (xyzError.equals(XyzError.UNAUTHORIZED)) {
      return HttpResponseStatus.UNAUTHORIZED;
    } else if (xyzError.equals(XyzError.FORBIDDEN)) {
      return HttpResponseStatus.FORBIDDEN;
    } else if (xyzError.equals(XyzError.TOO_MANY_REQUESTS)) {
      return HttpResponseStatus.TOO_MANY_REQUESTS;
    } else if (xyzError.equals(XyzError.TIMEOUT)) {
      return HttpResponseStatus.GATEWAY_TIMEOUT;
    } else if (xyzError.equals(XyzError.NOT_FOUND)) {
      return NOT_FOUND;
    }
    throw new IllegalArgumentException();
  }

  /**
   * Prepare XyzFeatureCollection response by extracting feature results from ModifyFeaturesResp object
   *
   * @param modifyResponse The object to extract feature results from
   * @return The XyzFeatureCollection response containing list of inserted/updated/delete features
   */
  public XyzFeatureCollection transformModifyResponse(@NotNull ModifyFeaturesResp modifyResponse) {
    final XyzFeatureCollection response = new XyzFeatureCollection();
    // add feature objects
    response.getFeatures().addAll(modifyResponse.getInserted());
    response.getFeatures().addAll(modifyResponse.getUpdated());
    response.getFeatures().addAll(modifyResponse.getDeleted());
    // add feature IDs
    for (final XyzFeature f : modifyResponse.getInserted()) {
      response.appendInsertId(f.getId());
    }
    for (final XyzFeature f : modifyResponse.getUpdated()) {
      response.appendUpdateId(f.getId());
    }
    for (final XyzFeature f : modifyResponse.getDeleted()) {
      response.appendDeleteId(f.getId());
    }

    return response;
  }

  /**
   * Send a response.
   *
   * @param routingContext The routing context for which to send the response.
   * @param responseType   The response type to return.
   * @param response       The response to send.
   * @return XyzResponse object representing actual response content
   */
  public XyzResponse sendXyzResponse(
      @NotNull RoutingContext routingContext,
      @Nullable HttpResponseType responseType,
      @NotNull XyzResponse response) {
    try {
      final String etag = response.getEtag();
      if (etag != null) {
        routingContext.response().putHeader(ETAG, etag);
      }
      if (response instanceof ErrorResponse er) {
        sendRawResponse(
            routingContext,
            mapErrorToHttpStatus(er.getError()),
            APPLICATION_JSON,
            Buffer.buffer(er.serialize()));
        return response;
      }
      if (response instanceof BinaryResponse br) {
        sendRawResponse(routingContext, OK, br.getMimeType(), Buffer.buffer(br.getBytes()));
        return response;
      }
      if (response instanceof NotModifiedResponse) {
        sendEmptyResponse(routingContext, NOT_MODIFIED);
        return response;
      }
      if (response instanceof XyzFeatureCollection fc && responseType == HttpResponseType.FEATURE) {
        // If we should only send back a single feature.
        final List<? extends XyzFeature> features = fc.getFeatures();
        if (features.size() == 0) {
          sendEmptyResponse(routingContext, OK);
          return response;
        } else {
          final String content = features.get(0).serialize();
          sendRawResponse(routingContext, OK, responseType, Buffer.buffer(content));
          return response;
        }
      }
      if (responseType == HttpResponseType.EMPTY) {
        sendEmptyResponse(routingContext, OK);
        return response;
      }
      sendRawResponse(routingContext, OK, responseType, Buffer.buffer(response.serialize()));
    } catch (Throwable t) {
      log.atError()
          .setMessage("Unexpected error while sending XYZ response")
          .setCause(t)
          .log();
      sendFatalErrorResponse(routingContext, "Unexpected failure while serializing response");
    }
    return response;
  }

  /**
   * Internal method to send back a response. The default content type will be {@code application/json}, except overridden via headers.
   *
   * @param routingContext The routing context to send the response to.
   * @param status         The HTTP status code to set.
   */
  public void sendEmptyResponse(@NotNull RoutingContext routingContext, @NotNull HttpResponseStatus status) {
    sendRawResponse(routingContext, status, null, null);
  }

  /**
   * Send back a fatal error, type {@link XyzError#EXCEPTION}.
   *
   * @param routingContext The routing context to send the response to.
   * @param errorMessage   The error message to return.
   */
  public void sendFatalErrorResponse(@NotNull RoutingContext routingContext, @NotNull String errorMessage) {
    assert FATAL_ERROR_MSG_PATTERN.matcher(errorMessage).matches();
    final String content = "{\n"
        + "\"type\": \"ErrorResponse\",\n"
        + "\"error\": \"Exception\",\n"
        + "\"errorMessage\": \"" + errorMessage + "\",\n"
        + "\"streamId\": \"" + AccessLogUtil.getStreamId(routingContext) + "\"\n"
        + "}";
    sendRawResponse(
        routingContext, HttpResponseStatus.INTERNAL_SERVER_ERROR, APPLICATION_JSON, Buffer.buffer(content));
  }

  /**
   * Internal method to send back a response. The default content type will be {@code application/json}, except overridden via headers.
   *
   * @param routingContext The routing context to send the response to.
   * @param status         The HTTP status code to set.
   * @param contentType    The content-type; if any.
   * @param content        The content; if any.
   */
  public void sendRawResponse(
      @NotNull RoutingContext routingContext,
      @NotNull HttpResponseStatus status,
      @Nullable CharSequence contentType,
      @Nullable Buffer content) {
    final HttpServerResponse httpResponse = routingContext.response();
    httpResponse.setStatusCode(status.code()).setStatusMessage(status.reasonPhrase());
    httpResponse.putHeader(STREAM_ID, AccessLogUtil.getStreamId(routingContext));
    if (content == null || content.length() == 0) {
      httpResponse.end();
    } else {
      if (contentType != null) {
        httpResponse.putHeader(CONTENT_TYPE, contentType);
        // See: https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/X-Content-Type-Options
        httpResponse.putHeader("X-Content-Type-Options", "nosniff");
      }
      httpResponse.end(content);
    }
    log.info("Returned Http status {}", status.code());
  }

  public @NotNull NakshaContext createNakshaContext(final @NotNull RoutingContext routingContext) {
    final NakshaContext ctx = new NakshaContext(AccessLogUtil.getStreamId(routingContext));
    // add streamInfo object to NakshaContext, which will be populated later during pipeline execution
    ctx.attachStreamInfo(AccessLogUtil.getStreamInfo(routingContext));
    // extract the JWT from authorization header
    final JWTPayload jwtPayload = extractJwtPayloadFromContext(routingContext);
    if (jwtPayload == null) {
      log.error("Cannot detect JWT payload in routing context: {}", routingContext);
      sendErrorResponse(routingContext, XyzError.UNAUTHORIZED, "No JWT payload found.");
    } else {
      // attach authorization info into context
      ctx.setAppId(jwtPayload.appId);
      ctx.setAuthor(jwtPayload.userId);
      ctx.setUrm(jwtPayload.urm);
    }
    return ctx;
  }
}
