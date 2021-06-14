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
import static com.here.xyz.hub.rest.Api.HeaderValues.STRICT_TRANSPORT_SECURITY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.REQUEST_ENTITY_TOO_LARGE;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
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

import com.here.xyz.hub.rest.Api;
import com.here.xyz.hub.rest.HttpException;
import com.here.xyz.hub.util.logging.LogUtil;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.impl.HttpStatusException;
import io.vertx.ext.web.validation.BadRequestException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public abstract class AbstractHttpServerVerticle extends AbstractVerticle {

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
   *
   * Call this method after all other routes are defined.
   *
   * @param router
   */
  protected void addDefaultHandlers(Router router) {
    //Add additional handler to the router
    router.route().failureHandler(createFailureHandler());
    router.route().order(0)
        .handler(createBodyHandler())
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

  protected void onRequestCancelled(RoutingContext context) {
    context.response().setStatusCode(CLIENT_CLOSED_REQUEST.code());
    context.response().setStatusMessage(CLIENT_CLOSED_REQUEST.reasonPhrase());
  }

  protected Handler<RoutingContext> createFailureHandler() {
    return context -> {
      String message = "A failure occurred during the execution.";
      if (context.failure() != null) {
        Throwable t = context.failure();
        if (t instanceof HttpStatusException) {
          //Transform Vert.x HTTP exception into ours
          HttpResponseStatus status = HttpResponseStatus.valueOf(((HttpStatusException) t).getStatusCode());
          if (status == UNAUTHORIZED)
            message = "Missing auth credentials.";
          t = new HttpException(status, message, t);
        }
        sendErrorResponse(context, t);
      }
      else {
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

  protected BodyHandler createBodyHandler() {
    BodyHandler bodyHandler = BodyHandler.create();
    if (Service.configuration != null && Service.configuration.MAX_UNCOMPRESSED_REQUEST_SIZE > 0) {
      // This check works only for multipart or url encoded content types, not for application/geo+json
      bodyHandler = bodyHandler.setBodyLimit(Service.configuration.MAX_UNCOMPRESSED_REQUEST_SIZE);
    }

    return bodyHandler;
  }

  /**
   * The max request size handler.
   */
  protected Handler<RoutingContext> createMaxRequestSizeHandler() {
    return context -> {
      if (Service.configuration != null && Service.configuration.MAX_UNCOMPRESSED_REQUEST_SIZE > 0) {
        if (context.getBody() != null && context.getBody().length() > Service.configuration.MAX_UNCOMPRESSED_REQUEST_SIZE) {
          sendErrorResponse(context, new HttpException(REQUEST_ENTITY_TOO_LARGE, "The request payload is bigger than the maximum allowed."));
        }
      }

      context.next();
    };
  }

  /**
   * The initial request handler.
   */
  protected Handler<RoutingContext> createReceiveHandler() {
    return context -> {
      if (context.request().getHeader(STREAM_ID) == null) {
        context.request().headers().add(STREAM_ID, RandomStringUtils.randomAlphanumeric(10));
      }

      //Log the request information.
      LogUtil.addRequestInfo(context);
      context.response().putHeader(STREAM_ID, context.request().getHeader(STREAM_ID));
      context.response().putHeader(STRICT_TRANSPORT_SECURITY, "max-age=" + TimeUnit.MINUTES.toSeconds(1));
      context.response().endHandler(ar -> onResponseEnd(context));
      context.addHeadersEndHandler(v -> headersEndHandler(context));
      context.next();
    };
  }

  protected static void headersEndHandler(RoutingContext context) {
    Map<String, Object> streamInfo;
    if (context != null && (streamInfo = context.get(STREAM_INFO_CTX_KEY)) != null) {
      String streamInfoValues = "";
      for (Entry<String, Object> e : streamInfo.entrySet())
        streamInfoValues += e.getKey() + "=" + e.getValue() + ";";

      context.response().putHeader(STREAM_INFO, streamInfoValues);
    }
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
   * Add support for cross origin requests.
   */
  protected CorsHandler createCorsHandler() {
    CorsHandler cors = CorsHandler.create(".*").allowCredentials(true);
    allowMethods.forEach(cors::allowedMethod);
    allowHeaders.stream().map(String::valueOf).forEach(cors::allowedHeader);
    exposeHeaders.stream().map(String::valueOf).forEach(cors::exposedHeader);
    return cors;
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
      Marker marker = Api.Context.getMarker(context);
      streamId = marker.getName();
      message = e.getMessage();
      if (e instanceof HttpException) {
        statusCode = ((HttpException) e).status.code();
        reasonPhrase = ((HttpException) e).status.reasonPhrase();
      }
      else if (e instanceof BadRequestException) {
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
