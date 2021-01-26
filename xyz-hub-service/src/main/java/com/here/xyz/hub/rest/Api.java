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

package com.here.xyz.hub.rest;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_VND_MAPBOX_VECTOR_TILE;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;
import static io.netty.handler.codec.http.HttpHeaderValues.TEXT_PLAIN;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_GATEWAY;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.GATEWAY_TIMEOUT;
import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.NO_CONTENT;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.ACCEPT_ENCODING;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.XYZHubRESTVerticle;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.connectors.models.BinaryResponse;
import com.here.xyz.hub.connectors.models.Space.CacheProfile;
import com.here.xyz.hub.rest.ApiParam.Query;
import com.here.xyz.hub.task.FeatureTask;
import com.here.xyz.hub.task.SpaceTask;
import com.here.xyz.hub.task.Task;
import com.here.xyz.hub.util.logging.AccessLog;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space.Internal;
import com.here.xyz.models.hub.Space.Public;
import com.here.xyz.models.hub.Space.WithConnectors;
import com.here.xyz.responses.CountResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.responses.HistoryStatisticsResponse;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.responses.changesets.CompactChangeset;
import io.netty.handler.codec.compression.ZlibWrapper;
import io.netty.handler.codec.http.HttpContentCompressor;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.MultiMap;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.CaseInsensitiveHeaders;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.RoutingContext;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.stream.Stream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Marker;
import org.apache.logging.log4j.MarkerManager.Log4jMarker;

public abstract class Api {

  private static final Logger logger = LogManager.getLogger();

  public static final int MAX_SERVICE_RESPONSE_SIZE = Service.configuration.MAX_SERVICE_RESPONSE_SIZE;
  public static final int MAX_HTTP_RESPONSE_SIZE = Service.configuration.MAX_HTTP_RESPONSE_SIZE;
  public static final HttpResponseStatus RESPONSE_PAYLOAD_TOO_LARGE = new HttpResponseStatus(513, "Response payload too large");
  public static final String RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE =
      "The response payload was too large. Please try to reduce the expected amount of data.";
  public static final HttpResponseStatus CLIENT_CLOSED_REQUEST = new HttpResponseStatus(499, "Client closed request");
  private static final String DEFAULT_GATEWAY_TIMEOUT_MESSAGE = "The storage connector exceeded the maximum time";
  private static final String DEFAULT_BAD_GATEWAY_MESSAGE = "The storage connector failed to execute the request";

  /**
   * Converts the given response into a {@link HttpException}.
   *
   * @param response the response to be converted.
   * @return the {@link HttpException} that reflects the response best.
   */
  public static HttpException responseToHttpException(final XyzResponse response) {
    if (response instanceof ErrorResponse) {
      return new HttpException(BAD_GATEWAY, ((ErrorResponse) response).getErrorMessage());
    }
    return new HttpException(BAD_GATEWAY, "Received invalid response of type '" + response.getClass().getSimpleName() + "'");
  }

  /**
   * If an empty response should be sent, then this method will either send an empty response or an error response. If the response is an
   * {@link ErrorResponse}, but an empty response was desired.
   *
   * @return true if a response was sent; false otherwise.
   */
  private boolean sendEmptyResponse(final Task task) {
    if (ApiResponseType.EMPTY == task.responseType) {
      if (task instanceof FeatureTask) {
        final FeatureTask featureTask = (FeatureTask) task;
        if (featureTask.getResponse() instanceof ErrorResponse) {
          final ErrorResponse errorResponse = (ErrorResponse) featureTask.getResponse();
          // Note: This is only a warning as it is generally not our fault, so its no real error in the service.
          logger.warn(task.getMarker(), "Received an error response: {}", errorResponse);
          if (XyzError.TIMEOUT.equals(errorResponse.getError())) {
            sendErrorResponse(task.context, GATEWAY_TIMEOUT, XyzError.TIMEOUT, DEFAULT_GATEWAY_TIMEOUT_MESSAGE);
          } else {
            sendErrorResponse(task.context, BAD_GATEWAY, errorResponse.getError(), DEFAULT_BAD_GATEWAY_MESSAGE);
          }
          return true;
        }
      }

      task.context.response().setStatusCode(NO_CONTENT.code()).end();
      return true;
    }

    return false;
  }

  /**
   * Internally used to send a "Not Modified" response when appropriate. In any case this method will set the e-tag header, if the task
   * response has generated an e-tag.
   *
   * @param task the task for which to generate the "Not Modified" response.
   * @return true if a response has been send; false if not.
   */
  private boolean sendNotModifiedResponseIfNoneMatch(final Task task) {
    //If the task has an ETag, set it in the HTTP header.
    //Set the ETag header
    if (task.getEtag() != null) {
      final RoutingContext context = task.context;
      final HttpServerResponse httpResponse = context.response();
      final MultiMap httpHeaders = httpResponse.headers();

      httpHeaders.add(HttpHeaders.ETAG, task.getEtag());

      //If the ETag didn't change, return "Not Modified"
      if (task.etagMatches()) {
        sendResponse(task, NOT_MODIFIED, null, null);
        return true;
      }
    }

    return false;
  }

  /**
   * Creates a response from the processed feature task and send it to the client.
   *
   * @param task the feature task that is finished processing and for which a response should be returned.
   */
  void sendResponse(final FeatureTask task) {
    if (sendEmptyResponse(task) || sendNotModifiedResponseIfNoneMatch(task)) {
      return;
    }

    final XyzResponse response = task.getResponse();
    if (response instanceof ErrorResponse) {
      final ErrorResponse errorResponse = (ErrorResponse) response;
      // Note: This is only a warning as it is generally not our fault, so its no real error in the service.
      logger.warn(task.getMarker(), "Received an error response: {}", errorResponse);
      if (XyzError.TIMEOUT.equals(errorResponse.getError())) {
        sendErrorResponse(task.context, GATEWAY_TIMEOUT, XyzError.TIMEOUT, DEFAULT_GATEWAY_TIMEOUT_MESSAGE);
      } else {
        sendErrorResponse(task.context, BAD_GATEWAY, errorResponse.getError(), DEFAULT_BAD_GATEWAY_MESSAGE);
      }
      return;
    }

    switch (task.responseType) {
      case FEATURE_COLLECTION: {
        if (response == null) {
          sendGeoJsonResponse(task, new FeatureCollection().serialize());
          return;
        }

        if (response instanceof FeatureCollection) {
          // Warning: We need to use "toString()" here and NOT Json.encode, because in fact the feature collection may be an
          // LazyParsedFeatureCollection and in that case only toString will work as intended!
          sendGeoJsonResponse(task, response.serialize());
          return;
        }
        break;
      }

      case MVT:
      case MVT_FLATTENED:
        if (response instanceof BinaryResponse) {
          sendMVTResponse(task, ((BinaryResponse) response).getBytes());
          return;
        }
        break;

      case FEATURE:
        if (response == null) {
          sendNotFoundJsonResponse(task);
          return;
        }

        if (response instanceof FeatureCollection) {
          try {
            final FeatureCollection collection = (FeatureCollection) response;

            if (collection.getFeatures() == null || collection.getFeatures().size() == 0) {
              sendNotFoundJsonResponse(task);
              return;
            }

            sendGeoJsonResponse(task, Json.encode(collection.getFeatures().get(0)));
          } catch (JsonProcessingException e) {
            logger.error(task.getMarker(), "The service received an invalid response and is unable to serialize it.", e);
            sendErrorResponse(task.context, INTERNAL_SERVER_ERROR, XyzError.EXCEPTION,
                "The service received an invalid response and is unable to serialize it.");
          }
          return;
        }
        break;

      case COUNT_RESPONSE:
        if (response instanceof CountResponse) {
          sendJsonResponse(task, Json.encode(response));
          return;
        }
        break;

      case CHANGESET_COLLECTION:
        if (response instanceof ChangesetCollection) {
          sendJsonResponse(task, Json.encode(response));
          return;
        }

      case COMPACT_CHANGESET:
        if (response instanceof CompactChangeset) {
          sendJsonResponse(task, Json.encode(response));
          return;
        }

      case STATISTICS_RESPONSE:
        if (response instanceof StatisticsResponse) {
          sendJsonResponse(task, Json.encode(response));
          return;
        }

      case HISTORY_STATISTICS_RESPONSE:
        if (response instanceof HistoryStatisticsResponse) {
          sendJsonResponse(task, Json.encode(response));
          return;
        }

      case EMPTY:
        sendEmptyResponse(task);
        return;
      default:
    }

    logger.warn(task.getMarker(), "Invalid response for request {}: {}, stack-trace: {}", task.responseType, response, new Exception());
    sendErrorResponse(task.context, BAD_GATEWAY, XyzError.EXCEPTION,
        "Received an invalid response from the storage connector, expected '" + task.responseType.name() + "', but received: '"
            + response.getClass().getSimpleName() + "'");
  }

  /**
   * Helper method which returns the marker for the JSON writer depending on which parameters the user has access in the response. These
   * output parameters are controlled by the task.view property and additionally by the accessConnectors
   *
   * @param view the view
   * @return the type
   */
  private Class<? extends Public> getViewType(final SpaceTask.View view) {
    switch (view) {
      case FULL:
        return Internal.class;
      case CONNECTOR_RIGHTS:
        return WithConnectors.class;
      default:
        return Public.class;
    }
  }

  /**
   * Creates a response from the processed space task and send it to the client.
   *
   * @param task the space task that is finished processing and for which a response should be returned.
   * @throws JsonProcessingException if serializing the content failed.
   */
  void sendResponse(final SpaceTask<?> task) throws JsonProcessingException {
    if (sendEmptyResponse(task) || sendNotModifiedResponseIfNoneMatch(task)) {
      return;
    }

    final Class<?> view = getViewType(task.view);
    switch (task.responseType) {
      case SPACE: {
        if (task.responseSpaces == null || task.responseSpaces.size() == 0) {
          sendNotFoundResponse(task);
          return;
        }

        final String geoJson = Json.mapper.writerWithView(view).writeValueAsString(task.responseSpaces.get(0));
        sendGeoJsonResponse(task, geoJson);
        return;
      }

      case SPACE_LIST: {
        if (task.responseSpaces == null || task.responseSpaces.size() == 0) {
          sendJsonResponse(task, Json.encode(Collections.EMPTY_LIST));
          return;
        }

        final String geoJson = Json.mapper.writerWithView(view).writeValueAsString(task.responseSpaces);
        sendJsonResponse(task, geoJson);
        return;
      }

      default:
    }

    // Invalid response.
    logger.error(task.getMarker(), "Invalid response for requested type {}: {}, stack-trace: {}", task.responseType,
        task.responseSpaces, new Exception());
    sendErrorResponse(task.context, INTERNAL_SERVER_ERROR, XyzError.EXCEPTION,
        "Internally generated invalid response, expected: " + task.responseType);
  }

  /**
   * Send an error response to the client when an exception occurred while processing a task.
   *
   * @param task the task for which to return an error response.
   * @param e the exception that should be used to generate an {@link ErrorResponse}, if null an internal server error is returned.
   */
  void sendErrorResponse(final Task task, final Exception e) {
    sendErrorResponse(task.context, e);
  }

  /**
   * Send an error response to the client when an exception occurred while processing a task.
   *
   * @param context the context for which to return an error response.
   * @param e the exception that should be used to generate an {@link ErrorResponse}, if null an internal server error is returned.
   */
  protected void sendErrorResponse(final RoutingContext context, final Exception e) {
    if (e instanceof HttpException) {
      final HttpException httpException = (HttpException) e;

      if (INTERNAL_SERVER_ERROR.code() != httpException.status.code()) {
        XyzError error;
        if (BAD_GATEWAY.code() == httpException.status.code()) {
          error = XyzError.BAD_GATEWAY;
        } else if (GATEWAY_TIMEOUT.code() == httpException.status.code()) {
          error = XyzError.TIMEOUT;
        } else if (BAD_REQUEST.code() == httpException.status.code()) {
          error = XyzError.ILLEGAL_ARGUMENT;
        } else {
          error = XyzError.EXCEPTION;
        }

        //This is an exception sent by intention and nothing special, no need for stacktrace logging.
        logger.warn("Error was handled by Api and will be sent as response: {}", httpException.status.code());
        sendErrorResponse(context, httpException, error);
        return;
      }
    }

    // This is an exception that is not done by intention.
    logger.error("Unintentional Error:", e);
    XYZHubRESTVerticle.sendErrorResponse(context, e);
  }

  /**
   * Send an error response to the client.
   *
   * @param context the routing context for which to return an error response.
   * @param status the HTTP status code to set.
   * @param error the error type that will become part of the {@link ErrorResponse}.
   * @param errorMessage the error message that will become part of the {@link ErrorResponse}.
   */
  private void sendErrorResponse(final RoutingContext context, final HttpResponseStatus status, final XyzError error,
      final String errorMessage) {
    context.response()
        .putHeader(CONTENT_TYPE, APPLICATION_JSON)
        .setStatusCode(status.code())
        .setStatusMessage(status.reasonPhrase())
        .end(new ErrorResponse()
            .withStreamId(Api.Context.getMarker(context).getName())
            .withError(error)
            .withErrorMessage(errorMessage).serialize());
  }

  /**
   * Send an error response to the client.
   *
   * @param context the routing context for which to return an error response.
   * @param httpError the HTTPException with all information
   * @param error the error type that will become part of the {@link ErrorResponse}.
   */
  private void sendErrorResponse(final RoutingContext context, final HttpException httpError, final XyzError error) {
    context.response()
        .putHeader(CONTENT_TYPE, APPLICATION_JSON)
        .setStatusCode(httpError.status.code())
        .setStatusMessage(httpError.status.reasonPhrase())
        .end(new ErrorResponse()
            .withStreamId(Api.Context.getMarker(context).getName())
            .withErrorDetails(httpError.errorDetails)
            .withError(error)
            .withErrorMessage(httpError.getMessage()).serialize());
  }

  /**
   * Returns an "Not Found" response to the client with http status 404.
   *
   * @param task the task for which to return a Not Found response.
   */
  private void sendNotFoundResponse(final Task task) {
    task.context.response()
        .setStatusCode(NOT_FOUND.code())
        .putHeader(HttpHeaders.CONTENT_TYPE, TEXT_PLAIN)
        .end(task.context.request().uri());
  }

  /**
   * Returns an "Not Found" in json format response to the client with http status 404.
   *
   * @param task the task for which to return a Not Found response.
   */
  private void sendNotFoundJsonResponse(final Task task) {
    sendErrorResponse(task, new HttpException(NOT_FOUND, "The requested resource does not exist."));
  }

  /**
   * Returns a response to the client with JSON content and status 200.
   *
   * @param task the task for which to return the JSON response.
   */
  private void sendJsonResponse(final Task<?, ?> task, final String json) {
    sendResponse(task, OK, APPLICATION_JSON, json.getBytes());
  }

  /**
   * Returns a response to the client with GeoJSON content and status 200.
   *
   * @param task the task for which to return the GeoJSON response.
   */
  private void sendGeoJsonResponse(final Task task, final String geoJson) {
    sendResponse(task, OK, APPLICATION_GEO_JSON, geoJson.getBytes());
  }

  /**
   * Returns a response to the client with MVT content and status 200.
   *
   * @param task the task for which to return the MVT response.
   */
  private void sendMVTResponse(final Task task, final byte[] mvt) {
    sendResponse(task, OK, APPLICATION_VND_MAPBOX_VECTOR_TILE, mvt);
  }

  private long getMaxResponseLength(final RoutingContext context) {
    return XYZHttpContentCompressor.isCompressionEnabled(context.request().getHeader(ACCEPT_ENCODING)) ?
        MAX_SERVICE_RESPONSE_SIZE : MAX_HTTP_RESPONSE_SIZE;
  }

  private void sendResponse(final Task task, HttpResponseStatus status, String contentType, final byte[] response) {
    HttpServerResponse httpResponse = task.context.response().setStatusCode(status.code());

    CacheProfile cacheProfile = task.getCacheProfile();
    if (cacheProfile.browserTTL > 0) {
      httpResponse.putHeader(HttpHeaders.CACHE_CONTROL, "private, max-age=" + (cacheProfile.browserTTL / 1000));
    }

    if (response == null || response.length == 0) {
      httpResponse.end();
    } else if (response.length > getMaxResponseLength(task.context)) {
      sendErrorResponse(task.context, new HttpException(RESPONSE_PAYLOAD_TOO_LARGE, RESPONSE_PAYLOAD_TOO_LARGE_MESSAGE));
    } else {
      httpResponse.putHeader(CONTENT_TYPE, contentType);
      httpResponse.end(Buffer.buffer(response));
    }
  }

  public static class HeaderValues {

    public static final String STREAM_ID = "Stream-Id";
    public static final String STREAM_INFO = "Stream-Info";
    public static final String APPLICATION_GEO_JSON = "application/geo+json";
    public static final String APPLICATION_JSON = "application/json";
    public static final String APPLICATION_VND_MAPBOX_VECTOR_TILE = "application/vnd.mapbox-vector-tile";
    public static final String APPLICATION_VND_HERE_FEATURE_MODIFICATION_LIST = "application/vnd.here.feature-modification-list";
    public static final String APPLICATION_VND_HERE_CHANGESET_COLLECTION = "application/vnd.here.changeset-collection";
    public static final String APPLICATION_VND_HERE_COMPACT_CHANGESET = "application/vnd.here.compact-changeset";
  }

  private static class XYZHttpContentCompressor extends HttpContentCompressor {

    private static final XYZHttpContentCompressor instance = new XYZHttpContentCompressor();

    static boolean isCompressionEnabled(String acceptEncoding) {
      if (acceptEncoding == null) {
        return false;
      }

      final ZlibWrapper wrapper = instance.determineWrapper(acceptEncoding);
      return wrapper == ZlibWrapper.GZIP || wrapper == ZlibWrapper.ZLIB;
    }
  }

  public static final class Context {

    private static final String MARKER = "marker";
    private static final String ACCESS_LOG = "accessLog";
    private static final String JWT = "jwt";
    private static final String QUERY_PARAMS = "queryParams";

    /**
     * Returns the log marker for the request.
     *
     * @return the marker or null, if no marker was found.
     */
    public static Marker getMarker(RoutingContext context) {
      if (context == null) {
        return null;
      }
      Marker marker = context.get(MARKER);
      if (marker == null) {
        marker = new Log4jMarker(context.request().getHeader(STREAM_ID));
        context.put(MARKER, marker);
      }
      return marker;
    }

    /**
     * Returns the access log object for this request.
     *
     * @param context the routing context.
     * @return the access log object
     */
    public static AccessLog getAccessLog(RoutingContext context) {
      if (context == null) {
        return null;
      }
      AccessLog accessLog = context.get(ACCESS_LOG);
      if (accessLog == null) {
        accessLog = new AccessLog();
        context.put(ACCESS_LOG, accessLog);
      }
      return accessLog;
    }

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
        payload = Json.mapper.convertValue(context.user().principal(), JWTPayload.class);
        context.put(JWT, payload);
      }

      return payload;
    }

    /**
     * Returns the custom parsed query parameters.
     *
     * Temporary solution until https://github.com/vert-x3/issues/issues/380 is resolved.
     */

    private static final String[] nonDecodeList = { Query.TAGS };

    static MultiMap getQueryParameters(RoutingContext context) {
      MultiMap queryParams = context.get(QUERY_PARAMS);
      if (queryParams != null) {
        return queryParams;
      }
      final MultiMap map = new CaseInsensitiveHeaders();

      String query = context.request().query();
      if (query != null && query.length() > 0) {
        String[] paramStrings = query.split("&");
        for (String paramString : paramStrings) {
          int eqDelimiter = paramString.indexOf("=");
          if (eqDelimiter > 0) {
            String key = paramString.substring(0, eqDelimiter);
            boolean decode = !ArrayUtils.contains(nonDecodeList,key);
            String rawValue = paramString.substring(eqDelimiter + 1);
            if (rawValue.length() > 0) {
              String[] values = rawValue.split(",");
              Stream.of(values).forEach(v -> {
                try {
                  map.add(key, (decode ? URLDecoder.decode(v, Charset.defaultCharset().name()) : v ));
                } catch (UnsupportedEncodingException ignored) {
                }
              });
            }
          }
        }
      }
      context.put(QUERY_PARAMS, map);
      return map;
    }
  }
}
