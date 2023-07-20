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
package com.here.xyz.hub;

import static com.here.naksha.lib.core.NakshaContext.currentLogger;
import static com.here.xyz.hub.NakshaRoutingContext.routingContextLogger;
import static com.here.xyz.hub.NakshaRoutingContext.sendErrorResponse;
import static com.here.xyz.hub.NakshaRoutingContext.sendRawResponse;
import static com.here.xyz.hub.rest.HeaderValues.STREAM_ID;
import static com.here.xyz.hub.rest.HeaderValues.STREAM_INFO;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
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

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.MIMEType;
import com.here.xyz.hub.auth.NakshaAuthProvider;
import com.here.xyz.hub.auth.NakshaJwtAuthHandler;
import com.here.xyz.hub.rest.ConnectorApi;
import com.here.xyz.hub.rest.FeatureApi;
import com.here.xyz.hub.rest.FeatureQueryApi;
import com.here.xyz.hub.rest.HistoryQueryApi;
import com.here.xyz.hub.rest.SpaceApi;
import com.here.xyz.hub.rest.SubscriptionApi;
import com.here.xyz.hub.rest.health.HealthApi;
import com.here.xyz.hub.task.NakshaTask;
import com.here.xyz.hub.util.logging.LogUtil;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.FileSystemAccess;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.openapi.RouterBuilderOptions;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The Naksha-Hub verticle. Can only be created by the Naksha-Hub. We deploy as many verticle as we have CPUs.
 */
@SuppressWarnings("unused")
public class NakshaHubVerticle extends AbstractVerticle {

  private static final HttpServerOptions SERVER_OPTIONS = new HttpServerOptions()
      .setCompressionSupported(true)
      .setDecompressionSupported(true)
      .setHandle100ContinueAutomatically(true)
      .setTcpQuickAck(true)
      .setTcpFastOpen(true)
      .setMaxInitialLineLength(16 * 1024 * 1024) // mb
      .setIdleTimeout(300);

  NakshaHubVerticle(@NotNull NakshaHub hub, int index) {
    this.hub = hub;
    this.index = index;

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

    if (hub.config.webRoot != null) {
      staticHandler = StaticHandler.create(FileSystemAccess.ROOT, hub.config.webRoot)
          .setIndexPage("index.html")
          .setDirectoryListing(true)
          .setIncludeHidden(false);
    } else {
      staticHandler = null;
    }
  }

  /**
   * The Naksha-Hub to which the verticle belongs.
   */
  protected final @NotNull NakshaHub hub;

  /**
   * The index in the {@link NakshaHub#verticles} array.
   */
  protected final int index;

  @Override
  public void start(final @NotNull Promise<Void> startPromise) throws Exception {
    RouterBuilder.create(vertx, "static/openapi.yaml").onComplete(ar -> {
      try {
        if (ar.failed()) {
          throw ar.cause();
        }
        final RouterBuilder rb = ar.result();
        init(rb);
        startPromise.complete();
      } catch (Throwable t) {
        currentLogger()
            .error(
                "An error occurred during the creation of the router from the Open API specification file.",
                t);
        startPromise.fail(t);
      }
    });
  }

  private final @NotNull CorsHandler corsHandler;
  private final @Nullable StaticHandler staticHandler;

  private void init(final @NotNull RouterBuilder rb) {
    rb.setOptions(new RouterBuilderOptions()
        .setContractEndpoint(RouterBuilderOptions.STANDARD_CONTRACT_ENDPOINT)
        .setRequireSecurityHandlers(false));
    new FeatureApi(rb);
    new FeatureQueryApi(rb);
    new SpaceApi(rb);
    new HistoryQueryApi(rb);
    new ConnectorApi(rb);
    new SubscriptionApi(rb);

    final AuthenticationHandler jwtHandler =
        new NakshaJwtAuthHandler(new NakshaAuthProvider(vertx, hub.authOptions), null);
    rb.securityHandler("Bearer", jwtHandler);

    // Note: Creates a router with pre-configured routes from Open-API YAML.
    final Router router = rb.createRouter();

    // Add default handlers to be executed before all other routes.
    final Route earlyRoute = router.route();
    earlyRoute
        .order(-1)
        .handler(this::onNewRequest)
        .handler(this::maxRequestSizeHandler)
        .handler(this.corsHandler)
        .handler(BodyHandler.create()
            .setBodyLimit(Integer.MAX_VALUE - 65535)
            .setHandleFileUploads(false)
            .setPreallocateBodyBuffer(true));

    // Add routes for the health API.
    new HealthApi(vertx, router);

    // https://github.com/vert-x3/vertx-web/issues/2182
    System.setProperty("io.vertx.web.router.setup.lenient", "true");

    // Static resources route.
    router.route("/hub/static/*").handler(this::onResourceRequest);

    // Optional: Web server.
    if (staticHandler != null) {
      currentLogger().debug("Serving extra web-root folder in file-system with location: {}", hub.config.webRoot);
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
    vertx.createHttpServer(SERVER_OPTIONS).requestHandler(router).listen(hub.config.httpPort, result -> {
      if (result.succeeded()) {
        currentLogger().info("HTTP Server started on port {}", hub.config.httpPort);
      } else {
        currentLogger().error("An error occurred, during the initialization of the server.", result.cause());
      }
    });
  }

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
        } catch (IOException e) {
          routingContextLogger(routingContext).info("File not found in resources: {}", relativePath, e);
          routingContext.next();
        } finally {
          promise.complete();
        }
      }));
    } catch (RejectedExecutionException t) {
      routingContextLogger(routingContext).warn("Failed to load resource, no more worker", t);
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
  protected void notFoundHandler(final @NotNull RoutingContext routingContext) {
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
      routingContextLogger(routingContext).info("The request was cancelled. No response has been sent.");
      onRequestCancelled(routingContext);
    }
    routingContextLogger(routingContext).info("{}", LogUtil.responseToLogEntry(routingContext));
    LogUtil.addResponseInfo(routingContext).end();
    LogUtil.writeAccessLog(routingContext);
  }

  private static final HttpResponseStatus CLIENT_CLOSED_REQUEST =
      new HttpResponseStatus(499, "Client closed request");

  private void onRequestCancelled(@NotNull RoutingContext routingContext) {
    final NakshaTask<?> task = AbstractTask.currentTask();
    if (task != null) {
      if (task.cancel()) {
        routingContextLogger(routingContext)
            .info("Successfully cancelled task for client side closed connection");
      } else {
        routingContextLogger(routingContext).info("Failed to cancel task for client side closed connection");
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

  /**
   * The handler to be called as first handler.
   *
   * @param routingContext The routing context.
   */
  private void onNewRequest(final @NotNull RoutingContext routingContext) {
    final String streamId = NakshaRoutingContext.routingContextStreamId(routingContext);
    LogUtil.addRequestInfo(routingContext);
    routingContext.response().putHeader(STREAM_ID, streamId);
    routingContext.response().putHeader(STRICT_TRANSPORT_SECURITY, "max-age=" + TimeUnit.MINUTES.toSeconds(1));
    routingContext.response().endHandler(v -> onResponseEnd(routingContext));
    routingContext.addHeadersEndHandler(v -> onHeadersEnd(routingContext));
    routingContext.next();
  }
}
