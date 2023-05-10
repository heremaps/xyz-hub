package com.here.xyz.hub;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;
import static io.vertx.core.http.HttpHeaders.ETAG;

import com.here.xyz.NakshaLogger;
import com.here.xyz.NanoTime;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.NakshaTask;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.NotModifiedResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.HttpException;
import io.vertx.ext.web.validation.BadRequestException;
import io.vertx.ext.web.validation.BodyProcessorException;
import io.vertx.ext.web.validation.ParameterProcessorException;
import io.vertx.ext.web.validation.impl.ParameterLocation;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A collection of helper methods for the routing context.
 */
public final class NakshaRoutingContext {

  private NakshaRoutingContext() {
  }

  private static final Pattern FATAL_ERROR_MSG_PATTERN = Pattern.compile("^[0-9a-zA-Z .-_]+$");
  private static final String NAKSHA_STREAM_ID = "nakshaStreamId";
  private static final String NAKSHA_START_NANOS = "nakshaStartNanos";
  private static final String STREAM_ID_PATTERN_TEXT = "^[a-zA-Z0-9_-]{10,32}$";
  private static final Pattern STREAM_ID_PATTERN = Pattern.compile(STREAM_ID_PATTERN_TEXT);

  /**
   * Send an error response for the given exception.
   *
   * @param routingContext The routing context for which to send the response.
   * @param throwable      The exception for which to send an error response.
   */
  public static void sendErrorResponse(@NotNull RoutingContext routingContext, @NotNull Throwable throwable) {
    try {
      final ErrorResponse response;
      if (throwable instanceof XyzErrorException) {
        response = ((XyzErrorException) throwable).toErrorResponse(routingContextStreamId(routingContext));
      } else {
        response = new ErrorResponse();
        response.setStreamId(routingContextStreamId(routingContext));
        response.setErrorMessage(throwable.getMessage());

        if (throwable instanceof ParameterError) {
          response.setError(XyzError.ILLEGAL_ARGUMENT);
        } else if (throwable instanceof HttpException e) {
          response.setError(XyzError.EXCEPTION);
        } else if (throwable instanceof BodyProcessorException e) {
          response.setError(XyzError.ILLEGAL_ARGUMENT);
          response.setErrorMessage("Failed to process body, reason: " + e.getMessage());
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
          routingContextLogger(routingContext).info("Exception processing body: {}. Body was: {}", e.getMessage(), bodyPart);
        } else if (throwable instanceof ParameterProcessorException e) {
          final ParameterLocation location = e.getLocation();
          final String paramName = location.lowerCaseIfNeeded(e.getParameterName());
          final String locationName = location.lowerCaseIfNeeded(location.name());
          response.setError(XyzError.ILLEGAL_ARGUMENT);
          response.setErrorMessage("Invalid request input parameter value for " + locationName + "-parameter '" + paramName + "'. "
              + "Reason: " + e.getErrorType());
        } else if (throwable instanceof BadRequestException) {
          response.setError(XyzError.ILLEGAL_ARGUMENT);
        } else {
          response.setError(XyzError.EXCEPTION);
        }
      }
      assert response.getStreamId() != null;
      assert response.getError() != null;
      assert response.getErrorMessage() != null;
      sendRawResponse(routingContext, OK, APPLICATION_JSON, Buffer.buffer(response.serialize()));
    } catch (Throwable t) {
      routingContextLogger(routingContext).error("Unexpected failure while generating error response", t);
      sendFatalErrorResponse(routingContext, t.getMessage());
    }
  }

  /**
   * Send an error response for the given exception.
   *
   * @param routingContext The routing context for which to send the response.
   * @param response      The error response to send.
   */
  public static void sendErrorResponse(@NotNull RoutingContext routingContext, @NotNull ErrorResponse response) {
    sendXyzResponse(routingContext, ApiResponseType.ERROR, response);
  }

  /**
   * Send back a fatal error, type {@link XyzError#EXCEPTION}.
   *
   * @param routingContext The routing context to send the response to.
   * @param errorMessage   The error message to return.
   */
  public static void sendFatalErrorResponse(@NotNull RoutingContext routingContext, @NotNull String errorMessage) {
    assert FATAL_ERROR_MSG_PATTERN.matcher(errorMessage).matches();
    final String content = "{\n"
        + "\"type\": \"ErrorResponse\",\n"
        + "\"error\": \"Exception\",\n"
        + "\"errorMessage\": \"" + errorMessage + "\",\n"
        + "\"streamId\": \"" + routingContextStreamId(routingContext) + "\"\n"
        + "}";
    sendRawResponse(routingContext, OK, APPLICATION_JSON, Buffer.buffer(content));
  }

  /**
   * Send a response.
   *
   * @param routingContext The routing context for which to send the response.
   * @param responseType   The response type to return.
   * @param response       The response to send.
   */
  public static void sendXyzResponse(
      @NotNull RoutingContext routingContext,
      @NotNull ApiResponseType responseType,
      @NotNull XyzResponse response
  ) {
    try {
      final String etag = response.getEtag();
      if (etag != null) {
        routingContext.response().putHeader(ETAG, etag);
      }
      if (response instanceof ErrorResponse) {
        sendRawResponse(routingContext, OK, APPLICATION_JSON, Buffer.buffer(response.serialize()));
        return;
      }
      if (response instanceof BinaryResponse br) {
        sendRawResponse(routingContext, OK, br.getMimeType(), Buffer.buffer(br.getBytes()));
        return;
      }
      if (response instanceof NotModifiedResponse) {
        sendEmptyResponse(routingContext, NOT_MODIFIED);
        return;
      }
      if (response instanceof FeatureCollection fc && responseType == ApiResponseType.FEATURE) {
        // If we should only send back a single feature.
        final List<@NotNull Feature> features = fc.getFeatures();
        if (features.size() == 0) {
          sendEmptyResponse(routingContext, OK);
        } else {
          final String content = features.get(0).serialize();
          sendRawResponse(routingContext, OK, responseType, Buffer.buffer(content));
        }
      }
      if (responseType == ApiResponseType.EMPTY) {
        sendEmptyResponse(routingContext, OK);
        return;
      }
      sendRawResponse(routingContext, OK, responseType, Buffer.buffer(response.serialize()));
    } catch (Throwable t) {
      routingContextLogger(routingContext).error("Unexpected failure while serializing response", t);
      sendFatalErrorResponse(routingContext, t.getMessage());
    }
  }

  /**
   * Internal method to send back a response. The default content type will be {@code application/json}, except overridden via headers.
   *
   * @param routingContext The routing context to send the response to.
   * @param status         The HTTP status code to set.
   */
  public static void sendEmptyResponse(@NotNull RoutingContext routingContext, @NotNull HttpResponseStatus status) {
    sendRawResponse(routingContext, status, null, null);
  }

  /**
   * Internal method to send back a response. The default content type will be {@code application/json}, except overridden via headers.
   *
   * @param routingContext The routing context to send the response to.
   * @param status         The HTTP status code to set.
   * @param contentType    The content-type; if any.
   * @param content        The content; if any.
   */
  public static void sendRawResponse(
      @NotNull RoutingContext routingContext,
      @NotNull HttpResponseStatus status,
      @Nullable CharSequence contentType,
      @Nullable Buffer content
  ) {
    final HttpServerResponse httpResponse = routingContext.response();
    httpResponse.setStatusCode(status.code()).setStatusMessage(status.reasonPhrase());
    httpResponse.putHeader(STREAM_ID, routingContextStreamId(routingContext));
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
  }

  /**
   * Returns the logger for the given routing context.
   *
   * @param routingContext The routing context; if any.
   * @return The logger.
   */
  public static @NotNull NakshaLogger routingContextLogger(@Nullable RoutingContext routingContext) {
    if (routingContext != null) {
      final NakshaTask<?> task = NakshaTask.get(routingContext);
      if (task != null) {
        return task.logger();
      }
      return NakshaLogger.currentLogger().with(routingContextStreamId(routingContext), routingContextStartNanos(routingContext));
    }
    // Note: This method will check if a task bound to the current thread and return the correct streamId.
    return NakshaLogger.currentLogger();
  }

  /**
   * Extracts the stream-id from the given routing context or creates a new one and attaches it to the routing context.
   *
   * @param routingContext The routing context.
   * @return The stream-id.
   */
  public static @NotNull String routingContextStreamId(@NotNull RoutingContext routingContext) {
    final Object raw = routingContext.get(NAKSHA_STREAM_ID);
    if (raw instanceof String streamId) {
      return streamId;
    }
    final @NotNull String streamId;
    final @Nullable String streamIdFromHttpHeader = routingContext.request().headers().get("Stream-Id");
    if (streamIdFromHttpHeader != null) {
      final Matcher matcher = STREAM_ID_PATTERN.matcher(streamIdFromHttpHeader);
      if (matcher.matches()) {
        streamId = streamIdFromHttpHeader;
      } else {
        streamId = RandomStringUtils.randomAlphanumeric(12);
      }
    } else {
      streamId = RandomStringUtils.randomAlphanumeric(12);
    }
    routingContext.put(NAKSHA_STREAM_ID, streamId);

    //noinspection StringEquality
    if (streamIdFromHttpHeader != null && streamIdFromHttpHeader != streamId) {
      // Note: "currentLogger" will invoke this method again, therefore we needed to store the stream-id into the routing context before!
      routingContextLogger(routingContext).warn("The given external stream-id is invalid: {}", streamIdFromHttpHeader);
    }
    return streamId;
  }

  /**
   * Extracts the start-nanos from the given routing context or creates a new one and attaches it to the routing context.
   *
   * @param routingContext The routing context.
   * @return The start-nanos.
   */
  public static long routingContextStartNanos(@NotNull RoutingContext routingContext) {
    final Object raw = routingContext.get(NAKSHA_START_NANOS);
    if (raw instanceof Long startNanos) {
      return startNanos;
    }
    // TODO: Can we extract the start nanos from the HTTP request?
    //       routingContext.request().headers().get("???");
    final long startNanos = NanoTime.now();
    routingContext.put(NAKSHA_START_NANOS, startNanos);
    return startNanos;
  }

  /**
   * Helper method to generate a string hash-map inline, for example: <pre>{@code
   * vertx_sendRawResponse(
   *   HttpResponseStatus.OK,
   *   stringMap(CONTENT_TYPE, APPLICATION_JSON, "X-Foo", "Bar"),
   *   Buffer.buffer("{}")
   *   );
   * }</pre>
   *
   * @param keyValueList The keys and values.
   * @return The generated map.
   */
  public static @NotNull Map<@NotNull String, @NotNull String> stringMap(@NotNull CharSequence... keyValueList) {
    assert keyValueList != null && (keyValueList.length & 1) == 0;
    final Map<@NotNull String, @NotNull String> map = new HashMap<>();
    int i = 0;
    while (i < keyValueList.length) {
      final String key = keyValueList[i++].toString();
      final String value = keyValueList[i++].toString();
      map.put(key, value);
    }
    return map;
  }

}
