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

package com.here.xyz.util.service;

import static com.here.xyz.util.Random.randomAlphaNumeric;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.AUTHOR_HEADER;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.STREAM_ID;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.STREAM_INFO;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.STRICT_TRANSPORT_SECURITY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.METHOD_NOT_ALLOWED;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.UNAUTHORIZED;
import static io.vertx.core.http.ClientAuth.REQUIRED;
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

import com.google.common.base.Strings;
import com.here.xyz.models.hub.jwt.JWTPayload;
import com.here.xyz.util.service.logging.LogUtil;
import com.here.xyz.util.web.XyzWebClient.ErrorResponseException;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.net.PemTrustOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.validation.BadRequestException;
import io.vertx.ext.web.validation.BodyProcessorException;
import io.vertx.ext.web.validation.ParameterProcessorException;
import io.vertx.ext.web.validation.impl.ParameterLocation;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;

public class BaseHttpServerVerticle extends AbstractVerticle {
  private static final Logger logger = LogManager.getLogger();
  public static final HttpServerOptions SERVER_OPTIONS = new HttpServerOptions()
      .setCompressionSupported(true)
      .setDecompressionSupported(true)
      .setHandle100ContinueAutomatically(true)
      .setTcpQuickAck(true)
      .setTcpFastOpen(true)
      .setMaxInitialLineLength(16 * 1024)
      .setIdleTimeout(300);
  public static final String STREAM_INFO_CTX_KEY = "streamInfo";
  public static final HttpResponseStatus CLIENT_CLOSED_REQUEST = new HttpResponseStatus(499, "Client closed request");
  private static final String JWT = "jwt";
  /**
   * The methods the client is allowed to use.
   */
  protected final List<HttpMethod> allowMethods = Arrays.asList(OPTIONS, GET, POST, PUT, DELETE, PATCH);
  /**
   * The headers, which can be exposed as part of the response.
   */
  protected final List<CharSequence> exposeHeaders = Arrays.asList(STREAM_ID, STREAM_INFO, ETAG);
  /**
   * The headers the client is allowed to send.
   */
  protected final List<CharSequence> allowHeaders = Arrays.asList(
      AUTHORIZATION, CONTENT_TYPE, USER_AGENT, IF_MODIFIED_SINCE, IF_NONE_MATCH, CACHE_CONTROL, STREAM_ID
  );

  /**
   * Returns the log marker for the request.
   *
   * @return the marker or null, if no marker was found.
   */
  public static JWTPayload getJWT(RoutingContext context) {
    if (context == null) {
      return null;
    }
    JWTPayload payload = context.get(JWT);
    if (payload == null && context.user() != null) {
      payload = DatabindCodec.mapper().convertValue(context.user().principal(), JWTPayload.class);
      context.put(JWT, payload);
    }

    return payload;
  }

  public static String getAuthor(RoutingContext context) {
    if (BaseConfig.instance.USE_AUTHOR_FROM_HEADER)
      return context.request().getHeader(AUTHOR_HEADER);
    return getJWT(context).aid;
  }

  /**
   * Creates and sends an error response to the client.
   */
  public static void sendErrorResponse(final RoutingContext context, Throwable exception) {
    final Marker marker = LogUtil.getMarker(context);

    //If the request was canceled, neither a response has to be sent nor the error should be logged.
    if (exception instanceof RequestCancelledException)
      return;
    if (exception instanceof IllegalStateException && exception.getMessage().startsWith("Request method must be one of"))
      exception = new HttpException(METHOD_NOT_ALLOWED, exception.getMessage(), exception);
    if (exception instanceof ErrorResponseException errorResponseException) {
      final HttpResponse<byte[]> errorResponse = errorResponseException.getErrorResponse();
      final HttpRequest failedRequest = errorResponse.request();
      logger.error("Error during upstream request - Performing {} {}. Upstream-ID: {}, Response:\n{}",
          failedRequest.method(), failedRequest.uri(), errorResponse.headers().firstValue(STREAM_ID).orElse(null),
          new String(errorResponse.body()));
    }

    ErrorMessage error;

    try {
      error = new ErrorMessage(context, exception);
      if (error.statusCode == 500) {
        error.message = null;
        logger.error(marker, "Sending error response: {} {}", error.statusCode, error.reasonPhrase, exception);
      }
      else
        logger.warn(marker, "Sending error response: {} {}", error.statusCode, error.reasonPhrase, exception);
    }
    catch (Exception e) {
      logger.error(marker, "Error {} while preparing error response {}", e, exception);
      logger.error(marker, "Error:", e);
      logger.error(marker, "Original error:", exception);
      error = new ErrorMessage();
    }

    context.response()
        .putHeader(CONTENT_TYPE, APPLICATION_JSON)
        .setStatusCode(error.statusCode)
        .setStatusMessage(error.reasonPhrase)
        .end(Json.encode(error));
  }

  /**
   * Returns the custom Stream-Info key to be added to the headers together with the original Stream-Info header.
   * When set, the stream info values will be duplicated in two different headers during response.
   * @return CUSTOM_STREAM_INFO_HEADER_NAME or null when not set.
   */
  protected static String getCustomStreamInfoKey() {
    if (BaseConfig.instance != null && !Strings.isNullOrEmpty(BaseConfig.instance.CUSTOM_STREAM_INFO_HEADER_NAME)) {
      return BaseConfig.instance.CUSTOM_STREAM_INFO_HEADER_NAME;
    }

    return null;
  }

  protected static void headersEndHandler(RoutingContext context, String customStreamInfoKey) {
    Map<String, Object> streamInfo;
    if (context != null && (streamInfo = context.get(STREAM_INFO_CTX_KEY)) != null) {
      String streamInfoValues = "";
      for (Entry<String, Object> e : streamInfo.entrySet())
        streamInfoValues += e.getKey() + "=" + e.getValue() + ";";

      context.response().putHeader(STREAM_INFO, streamInfoValues);
      if (customStreamInfoKey != null) {
        context.response().putHeader(customStreamInfoKey, streamInfoValues);
      }
    }
  }

  /**
   * Add support for cross-origin requests.
   */
  protected CorsHandler createCorsHandler() {
    CorsHandler cors = CorsHandler.create(".*").allowCredentials(true);
    allowMethods.forEach(cors::allowedMethod);
    allowHeaders.stream().map(String::valueOf).forEach(cors::allowedHeader);
    exposeHeaders.stream().map(String::valueOf).forEach(cors::exposedHeader);
    return cors;
  }

  public Future<Void> createHttpServer(int port, Router router) {
    return createHttpServerWithTls(port, router, null, null);
  }

  public Future<Void> createHttpServerWithTls(int port, Router router, String serverPemKey, String serverPemCert) {
    return createHttpServerWithMutualTls(port, router, serverPemKey, serverPemCert, null);
  }

  public Future<Void> createHttpServerWithMutualTls(int port, Router router, String serverPemKey, String serverPemCert, String clientAuthPemTrustCertChain) {
    Promise<Void> promise = Promise.promise();

    HttpServerOptions serverOptions = new HttpServerOptions(SERVER_OPTIONS);

    if (serverPemKey != null && serverPemCert != null)
      serverOptions
          .setSsl(true)
          .setKeyCertOptions(
              new PemKeyCertOptions()
                  .setKeyValue(Buffer.buffer(serverPemKey))
                  .setCertValue(Buffer.buffer(serverPemCert))
          );

    if (clientAuthPemTrustCertChain != null)
      serverOptions
          .setClientAuth(REQUIRED)
          .setTrustOptions(
              new PemTrustOptions().addCertValue(Buffer.buffer(clientAuthPemTrustCertChain))
          );


    vertx.createHttpServer(serverOptions)
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

  protected void onRequestCancelled(RoutingContext context) {
    context.response().setStatusCode(CLIENT_CLOSED_REQUEST.code());
    context.response().setStatusMessage(CLIENT_CLOSED_REQUEST.reasonPhrase());
  }

  /**
   * The final response handler.
   */
  protected void onResponseEnd(RoutingContext context) {
    final Marker marker = LogUtil.getMarker(context);
    if (!context.response().headWritten()) {
      //The response was closed (e.g., by the client) before it could be written
      logger.info(marker, "The request was cancelled. No response has been sent.");
      onRequestCancelled(context);
    }
    logger.info(marker, "{}", LogUtil.responseToLogEntry(context));
    LogUtil.addResponseInfo(context).end();
    LogUtil.writeAccessLog(context);
  }

  /**
   * The default NOT FOUND handler.
   */
  protected Handler<RoutingContext> createNotFoundHandler() {
    return context -> sendErrorResponse(context, new HttpException(NOT_FOUND, "The requested resource does not exist."));
  }

  protected Handler<RoutingContext> createFailureHandler() {
    return context -> {
      String message = "A failure occurred during the execution.";
      if (context.failure() != null) {
        Throwable t = context.failure();
        if (t instanceof io.vertx.ext.web.handler.HttpException) {
          //Transform Vert.x HTTP exception into ours
          HttpResponseStatus status = HttpResponseStatus.valueOf(((io.vertx.ext.web.handler.HttpException) t).getStatusCode());
          if (status == UNAUTHORIZED)
            message = "Missing auth credentials.";
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
        }
        else if (t instanceof ParameterProcessorException) {
          ParameterLocation location = ((ParameterProcessorException) t).getLocation();
          String paramName = ((ParameterProcessorException) t).getParameterName();
          sendErrorResponse(context, new HttpException(BAD_REQUEST, "Invalid request input parameter value for "
              + location.name().toLowerCase() + "-parameter \"" + location.lowerCaseIfNeeded(paramName) + "\". Reason: "
              + ((ParameterProcessorException) t).getErrorType()));
        }
        else if (t instanceof BadRequestException)
          sendErrorResponse(context, new HttpException(BAD_REQUEST, "Invalid request."));
        else
          sendErrorResponse(context, t);
      }
      else {
        HttpResponseStatus status = context.statusCode() >= 400 ? HttpResponseStatus.valueOf(context.statusCode()) : INTERNAL_SERVER_ERROR;
        sendErrorResponse(context, new HttpException(status, message));
      }
    };
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
    var bodyHandler = BodyHandler.create();
    bodyHandler.setBodyLimit(-1); // set for unlime by body size, default value 10 megabytes
    // starts at the 2nd route, since the first one is automatically added from openapi's RouterBuilder.createRouter
    router.route().order(1)
        .handler(createCorsHandler())
        .handler(bodyHandler)
        .handler(createReceiveHandler())
        .handler(createMaxRequestSizeHandler());
  }

  /**
   * The max request size handler.
   */
  protected Handler<RoutingContext> createMaxRequestSizeHandler() {
    return context -> context.next();
  }

  /**
   * The initial request handler.
   */
  protected Handler<RoutingContext> createReceiveHandler() {
    final long maxAge = TimeUnit.MINUTES.toSeconds(1);
    final String customStreamInfoKey = getCustomStreamInfoKey();

    return context -> {
      if (context.request().getHeader(STREAM_ID) == null) {
        context.request().headers().add(STREAM_ID, randomAlphaNumeric(10));
      }

      //Log the request information.
      LogUtil.addRequestInfo(context);
      context.response().putHeader(STREAM_ID, context.request().getHeader(STREAM_ID));
      context.response().putHeader(STRICT_TRANSPORT_SECURITY, "max-age=" + maxAge);
      context.response().endHandler(ar -> onResponseEnd(context));
      context.addHeadersEndHandler(v -> BaseHttpServerVerticle.headersEndHandler(context, customStreamInfoKey));
      context.next();
    };
  }

  public static class HeaderValues {
    public static final String STREAM_ID = "Stream-Id";
    public static final String STREAM_INFO = "Stream-Info";
    public static final String STRICT_TRANSPORT_SECURITY = "Strict-Transport-Security";
    public static final String APPLICATION_GEO_JSON = "application/geo+json";
    public static final String APPLICATION_JSON = "application/json";
    public static final String TEXT_PLAIN = "text/plain";
    public static final String APPLICATION_VND_MAPBOX_VECTOR_TILE = "application/vnd.mapbox-vector-tile";
    public static final String APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST = "application/vnd.here.feature-modification-list";
    public static final String AUTHOR_HEADER = "Author";
  }

  public static class RequestCancelledException extends RuntimeException {

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
      Marker marker = LogUtil.getMarker(context);
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

  public static class ValidationException extends Exception {
      public ValidationException(String message) {
          super(message);
      }

      public ValidationException(String message, Exception cause) {
          super(message, cause);
      }
  }
}
