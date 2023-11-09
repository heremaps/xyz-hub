/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
import static com.here.naksha.lib.core.exceptions.UncheckedException.cause;
import static com.here.naksha.lib.core.models.XyzError.ILLEGAL_ARGUMENT;
import static com.here.naksha.lib.core.util.MIMEType.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.AUTHORIZATION;
import static io.vertx.core.http.HttpHeaders.CACHE_CONTROL;
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

import com.here.naksha.app.service.AbstractNakshaHubVerticle;
import com.here.naksha.app.service.NakshaApp;
import com.here.naksha.app.service.http.apis.*;
import com.here.naksha.app.service.http.auth.NakshaJwtAuthHandler;
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
import com.here.naksha.lib.hub.NakshaHubConfig;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.FileSystemAccess;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.handler.StaticHandler;
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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;
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

  private static final HttpServerOptions SERVER_OPTIONS = new HttpServerOptions()
      .setCompressionSupported(true)
      .setDecompressionSupported(true)
      .setHandle100ContinueAutomatically(true)
      .setTcpQuickAck(true)
      .setTcpFastOpen(true)
      .setMaxInitialLineLength(16 * 1024 * 1024) // mb
      .setIdleTimeout(300);

  /**
   * Creates a new verticle. Each verticle will be bound to a single IO worker.
   *
   * @param naksha The Naksha-Hub.
   * @param index  The verticle index.
   */
  public NakshaHttpVerticle(@NotNull INaksha naksha, int index, @NotNull NakshaApp app) {
    super(naksha, index, app);

    corsHandler = CorsHandler.create().allowCredentials(true); // .addRelativeOrigin(".*") <-- Not needed, default
    // The methods the client allowed to use.
    final List<HttpMethod> allowMethods = Arrays.asList(OPTIONS, GET, POST, PUT, DELETE, PATCH);
    allowMethods.forEach(corsHandler::allowedMethod);
    // The headers the client allowed to send.
    final List<CharSequence> allowHeaders = Arrays.asList(
        AUTHORIZATION, CONTENT_TYPE, USER_AGENT, IF_MODIFIED_SINCE, IF_NONE_MATCH, CACHE_CONTROL, STREAM_ID);
    allowHeaders.stream().map(String::valueOf).forEach(corsHandler::allowedHeader);
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
    RouterBuilder.create(vertx, "static/openapi.yaml").onComplete(ar -> {
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

        // TODO: Port the JWT authentication handler.
        final AuthenticationHandler jwtHandler = new NakshaJwtAuthHandler(app().authProvider, null);
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

        // Static resources route.
        router.route("/hub/static/*").handler(this::onResourceRequest);

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

        // Add the HTTP server.
        // When several HTTP servers listen on the same port, vert.x orchestrates the request handling using a
        // round-robin strategy.
        //
        // https://vertx.io/docs/vertx-core/java/#_server_sharing
        vertx.createHttpServer(SERVER_OPTIONS).requestHandler(router).listen(hubConfig.httpPort, result -> {
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
    if (!path.startsWith("/hub/static/")) {
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
  private void onHeadersEnd(final @NotNull RoutingContext routingContext) {}

  /**
   * An end handler for the response. This will be called when the response is disposed to allow consistent cleanup of the response.
   */
  private void onResponseEnd(final @NotNull RoutingContext routingContext) {
    if (!routingContext.response().headWritten()) {
      // The response closed e.g. by the client, before it could be written.
      log.info("The request was cancelled. No response has been sent.");
      onRequestCancelled(routingContext);
    }
    // TODO: We need to rewrite the LogUtil, because we now (with SLF4J) have structured logs:
    //       log.atInfo().setMessage("foo").addKeyValue("reqInfo", reqInfo)...
    // routingContextLogger(routingContext).info("{}", LogUtil.responseToLogEntry(routingContext));
    // LogUtil.addResponseInfo(routingContext).end();
    // LogUtil.writeAccessLog(routingContext);
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
    routingContext.response().putHeader(STREAM_ID, streamId(routingContext));
    routingContext.response().putHeader(STRICT_TRANSPORT_SECURITY, "max-age=" + TimeUnit.MINUTES.toSeconds(1));
    // TODO: Add request information, first read in onResponseEnd.
    // LogUtil.addRequestInfo(routingContext);
    routingContext.response().endHandler(v -> onResponseEnd(routingContext));
    routingContext.addHeadersEndHandler(v -> onHeadersEnd(routingContext));
    routingContext.next();
  }

  private static final Pattern FATAL_ERROR_MSG_PATTERN = Pattern.compile("^[0-9a-zA-Z.-_\\-]+$");

  /**
   * Returns the stream-identifier for this routing context.
   *
   * @param routingContext The routing context.
   * @return The stream-identifier for this routing context.
   */
  public @NotNull String streamId(@NotNull RoutingContext routingContext) {
    if (routingContext.get(STREAM_ID) instanceof String streamId) {
      return streamId;
    }
    final MultiMap headers = routingContext.request().headers();
    String streamId = headers.get(STREAM_ID);
    if (streamId != null && !FATAL_ERROR_MSG_PATTERN.matcher(streamId).matches()) {
      log.atInfo()
          .setMessage("Received invalid HTTP header 'Stream-Id', the provided value '{}' is not allowed")
          .addArgument(streamId)
          .log();
      streamId = null;
    }
    if (streamId == null) {
      streamId = RandomStringUtils.randomAlphanumeric(12);
    }
    routingContext.put(STREAM_ID, streamId);
    return streamId;
  }

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
    final ErrorResponse response = new ErrorResponse(xyzError, message, streamId(routingContext));
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
    final String streamId = streamId(routingContext);
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
      httpStatus = mapErrorToHttpStatus(response.getError());
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
    return switch (xyzError) {
      case EXCEPTION -> HttpResponseStatus.INTERNAL_SERVER_ERROR;
      case NOT_IMPLEMENTED -> HttpResponseStatus.NOT_IMPLEMENTED;
      case ILLEGAL_ARGUMENT -> HttpResponseStatus.BAD_REQUEST;
      case PAYLOAD_TOO_LARGE -> HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
      case BAD_GATEWAY -> HttpResponseStatus.BAD_GATEWAY;
      case CONFLICT -> HttpResponseStatus.CONFLICT;
      case UNAUTHORIZED -> HttpResponseStatus.UNAUTHORIZED;
      case FORBIDDEN -> HttpResponseStatus.FORBIDDEN;
      case TOO_MANY_REQUESTS -> HttpResponseStatus.TOO_MANY_REQUESTS;
      case TIMEOUT -> HttpResponseStatus.GATEWAY_TIMEOUT;
      case NOT_FOUND -> HttpResponseStatus.NOT_FOUND;
    };
  }

  /**
   * Prepare XyzFeatureCollection response by extracting feature results from ModifyFeaturesResp object
   *
   * @param modifyResponse       The object to extract feature results from
   * @return The XyzFeatureCollection response containing list of inserted/updated/delete features
   */
  public XyzFeatureCollection transformModifyResponse(@NotNull ModifyFeaturesResp modifyResponse) {
    final XyzFeatureCollection response = new XyzFeatureCollection();
    // add feature objects
    response.getFeatures().addAll(modifyResponse.inserted());
    response.getFeatures().addAll(modifyResponse.updated());
    response.getFeatures().addAll(modifyResponse.deleted());
    // add feature IDs
    for (final XyzFeature f : modifyResponse.inserted()) {
      response.appendInsertId(f.getId());
    }
    for (final XyzFeature f : modifyResponse.updated()) {
      response.appendUpdateId(f.getId());
    }
    for (final XyzFeature f : modifyResponse.deleted()) {
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
        + "\"streamId\": \"" + streamId(routingContext) + "\"\n"
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
    httpResponse.putHeader(STREAM_ID, streamId(routingContext));
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
    final NakshaContext ctx = new NakshaContext(streamId(routingContext));
    ctx.setAppId(hubConfig.appId);
    // TODO : Author to be set based on JWT token.
    // ctx.setAuthor();
    return ctx;
  }
}
