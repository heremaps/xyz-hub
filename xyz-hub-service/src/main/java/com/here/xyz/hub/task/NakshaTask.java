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

package com.here.xyz.hub.task;

import static com.here.xyz.hub.rest.Api.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.Api.HeaderValues.STREAM_ID;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_MODIFIED;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import com.here.xyz.AbstractTask;
import com.here.xyz.NanoTime;
import com.here.xyz.XyzLogger;
import com.here.xyz.events.Event;
import com.here.xyz.events.feature.LoadFeaturesEvent;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.hub.auth.ActionMatrix;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.params.XyzHubQueryParameters;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.util.logging.AccessLog;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.BinaryResponse;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.NotModifiedResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.JsonUtils;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.MIMEHeader;
import io.vertx.ext.web.RoutingContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The XYZ-Hub task is the base of all tasks.
 *
 * @param <EVENT> The event type to execute.
 */
@SuppressWarnings({"SameParameterValue", "unused"})
public abstract class NakshaTask<EVENT extends Event> extends AbstractTask {

  /**
   * The key used in the routing context to attach a task to a routing context.
   */
  private static final String NAKSHA_ROUTING_CONTEXT = "nakshaTask";

  /**
   * Tries to return the Naksha task bound to the given routing context.
   *
   * @param routingContext The routing context to query.
   * @return The Naksha task attached or {@code null}, if no Naksha tasks attached.
   */
  public static @Nullable NakshaTask<?> get(@NotNull RoutingContext routingContext) {
    final Object raw = routingContext.get(NAKSHA_ROUTING_CONTEXT);
    //noinspection rawtypes
    return raw instanceof NakshaTask task ? task : null;
  }

  /**
   * Returns the logger for the given routing context.
   *
   * @param routingContext The routing context; if any.
   * @return The logger.
   */
  public static @NotNull XyzLogger currentLogger(@Nullable RoutingContext routingContext) {
    if (routingContext != null) {
      final NakshaTask<?> task = NakshaTask.get(routingContext);
      if (task != null) {
        return task.logger();
      }
      return XyzLogger.currentLogger().with(getStreamId(routingContext), getStartNanos(routingContext));
    }
    // Note: This method will check if a task bound to the current thread and return the correct streamId.
    return XyzLogger.currentLogger();
  }

  private static final String NAKSHA_STREAM_ID = "nakshaStreamId";
  private static final String NAKSHA_START_NANOS = "nakshaStartNanos";
  private static final String STREAM_ID_PATTERN_TEXT = "^[a-zA-Z0-9_-]{10,32}$";
  private static final Pattern STREAM_ID_PATTERN = Pattern.compile(STREAM_ID_PATTERN_TEXT);
  private static final Pattern IF_NONE_MATCH_PATTERN = Pattern.compile("^[a-zA-Z0-9/_-]{10,}$");

  /**
   * The routing context; only set for tasks initialized from a routing context via
   * {@link #initEventFromRoutingContext(RoutingContext, ApiResponseType)}.
   */
  private @Nullable RoutingContext routingContext;

  /**
   * The response type that should be produced by this task; only set for tasks initialized from a routing context via
   * {@link #initEventFromRoutingContext(RoutingContext, ApiResponseType)}.
   */
  private @Nullable ApiResponseType responseType;

  /**
   * The query parameters; only set for tasks initialized from a routing context via
   * {@link #initEventFromRoutingContext(RoutingContext, ApiResponseType)}.
   */
  protected @Nullable XyzHubQueryParameters queryParameters;

  /**
   * The main event to be processed by the task, created by the constructor.
   */
  protected @NotNull EVENT event;

  /**
   * The request matrix, by default an empty matrix that means no rights required to execute the task.
   */
  protected @NotNull XyzHubActionMatrix requestMatrix;

  /**
   * Extracts the stream-id from the given routing context or creates a new one and attaches it to the routing context.
   *
   * @param routingContext The routing context.
   * @return The stream-id.
   */
  public static @NotNull String getStreamId(@NotNull RoutingContext routingContext) {
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
      currentLogger(routingContext).warn("The given external stream-id is invalid: {}", streamIdFromHttpHeader);
    }
    return streamId;
  }

  /**
   * Extracts the start-nanos from the given routing context or creates a new one and attaches it to the routing context.
   *
   * @param routingContext The routing context.
   * @return The start-nanos.
   */
  public static long getStartNanos(@NotNull RoutingContext routingContext) {
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
   * Returns the best response-type, dependent on the client selection.
   *
   * @param routingContext The routing context.
   * @param allowedTypes   All response types to be allowed with the first one being the one used as default.
   * @return The response type.
   */
  private static @NotNull ApiResponseType extractResponseType(
      @NotNull RoutingContext routingContext,
      @NotNull ApiResponseType... allowedTypes
  ) {
    if (allowedTypes == null || allowedTypes.length == 0) {
      // If no type allowed, the response must be empty.
      return ApiResponseType.EMPTY;
    }
    // If we have multiple allowed types, then review all types the client accepts and pick the best matching one.
    if (allowedTypes.length > 1) {
      final List<MIMEHeader> accept = routingContext.parsedHeaders().accept();
      for (final @NotNull MIMEHeader mimeType : accept) {
        for (final ApiResponseType allowedType : allowedTypes) {
          if (mimeType.isMatchedBy(allowedType.mimeType)) {
            return allowedType;
          }
        }
      }
    }
    // If we can't fulfill the wishlist of the client, use the first allowed (major) type.
    return allowedTypes[0];
  }

  /**
   * Start a Naksha task directly from the REST API. Internally creates a new task, binds the routing-context and response-type, add a
   * listener to be able to send back the response and eventually {@link #start() starts} the worker thread of this task. This causes the
   * {@link #init()} method to invoke the {@link #initEventFromRoutingContext(RoutingContext, ApiResponseType)} method before
   * {@link #execute()} is called.
   * <p>
   * By moving the {@link #initEventFromRoutingContext(RoutingContext, ApiResponseType)} into the worker thread we can distribute the
   * parsing effort of the query string and payload into own worker threads, instead of having to do all the work in the event loop. Apart
   * of this we may even perform blocking requests, when needed.
   * <p>
   * Technically a task does not need to be bound directly to a routing-context, for example when being created as sub-task by another task
   * or when executed as background job. However, for this purpose the event of the task must be manually setup.
   *
   * @param taskClass      The class of the task to execute.
   * @param routingContext The routing context.
   * @param responseTypes  The response types.
   */
  public static void start(
      @NotNull Class<? extends NakshaTask<?>> taskClass,
      @NotNull RoutingContext routingContext,
      @NotNull ApiResponseType... responseTypes) {
    final String streamId = getStreamId(routingContext);
    final ApiResponseType apiResponseType = extractResponseType(routingContext, responseTypes);
    XyzLogger logger = null;
    try {
      final NakshaTask<?> task = taskClass.getConstructor(String.class).newInstance(streamId);
      task.addListener(task::sendResponse);
      task.routingContext = routingContext;
      task.responseType = apiResponseType;
      try {
        task.start();
      } catch (Throwable t) {
        task.sendResponse(task.errorResponse(t));
      }
    } catch (Throwable t) {
      currentLogger(routingContext).error("Failed to create task: {}", taskClass.getName(), t);
      rcSendFatalError(routingContext, streamId, "Failed to create task: " + taskClass.getSimpleName());
    }
  }

  protected NakshaTask(@Nullable String streamId) {
    super(streamId);
    this.event = createEvent();
    this.requestMatrix = new XyzHubActionMatrix();
  }

  /**
   * Creates a new main event for this task. Internally a task may generate multiple sub-events and send them through the pipeline. For
   * example a {@link ModifyFeaturesEvent} requires a {@link LoadFeaturesEvent} pre-flight event, some other events may require other
   * pre-flight request.
   * <p>
   * The constructor invokes this method.
   *
   * @return a new main event for this task.
   */
  abstract protected @NotNull EVENT createEvent();

  /**
   * Initialize the task from the given routing context. Must be overridden by extending tasks do setup all parameters. The default
   * {@link #init()} method will call this method, and therefore this method may perform blocking IO calls if needed.
   *
   * @param routingContext The routing context to use to initialize this task.
   * @param responseType   The expected response type.
   * @throws ParameterError If any error happened while parsing query parameters or content.
   */
  protected void initEventFromRoutingContext(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType)
      throws ParameterError {
    // Note: This is not bullet-proof, because it is not concurrency poof.
    final NakshaTask<?> existing = get(routingContext);
    if (existing != null && existing != this) {
      throw new ParameterError("The given routing context is already bound to a Naksha task");
    }
    routingContext.put(NAKSHA_ROUTING_CONTEXT, this);
    streamId = getStreamId(routingContext);
    startNanos = getStartNanos(routingContext);
    event.setStreamId(streamId);

    final String ifNoneMatch = routingContext.request().headers().get("If-None-Match");
    if (ifNoneMatch != null) {
      final Matcher matcher = IF_NONE_MATCH_PATTERN.matcher(ifNoneMatch);
      if (matcher.matches()) {
        event.setIfNoneMatch(ifNoneMatch);
      } else {
        logger().info("The externally provided 'If-None-Match' contains illegal characters, ignoring it: {}", ifNoneMatch);
      }
    }

    final User user = routingContext.user();
    if (user != null) {
      final JWTPayload jwt = DatabindCodec.mapper().convertValue(user.principal(), JWTPayload.class);
      setJwt(jwt);
    }

    queryParameters = new XyzHubQueryParameters(routingContext.request().query());
    setResponseType(responseType);
  }

  /**
   * Invokes the {@link #initEventFromRoutingContext(RoutingContext, ApiResponseType)} method, when appropriate.
   *
   * @throws Throwable If any error occurred.
   */
  @Override
  protected void init() throws Throwable {
    if (routingContext != null && responseType != null) {
      initEventFromRoutingContext(routingContext, responseType);
    }
  }

  /**
   * Change the response type.
   *
   * @param responseType The new response type.
   */
  protected void setResponseType(@NotNull ApiResponseType responseType) {
    this.responseType = responseType;
  }

  /**
   * Returns the current response type.
   *
   * @return The current response type.
   */
  protected @NotNull ApiResponseType getResponseType() {
    return responseType != null ? responseType : ApiResponseType.FEATURE_COLLECTION;
  }

  /**
   * Returns the payload of the JWT Token.
   *
   * @return the payload of the JWT Token; if any.
   */
  public @Nullable JWTPayload getJwt() {
    return getAttachment(JWTPayload.class);
  }

  /**
   * Sets the JWT token against which {@link #sendAuthorizedEvent(Event, XyzHubActionMatrix)} will authorize the given request matrix.
   *
   * @param jwt the JWT payload to set.
   */
  public void setJwt(@Nullable JWTPayload jwt) {
    final EVENT event = createEvent();
    if (jwt == null) {
      attachments().remove(JWTPayload.class);
      event.setAid(null);
      event.setAuthor(null);
      event.setMetadata(null);
    } else {
      setAttachment(jwt);
      event.setAid(jwt.aid);
      event.setAuthor(jwt.user);
      event.setMetadata(JsonUtils.deepCopy(jwt.metadata));
    }
  }

  /**
   * Returns the access log object for this request.
   *
   * @return the access log object.
   */
  protected @NotNull AccessLog accessLog() {
    return getOrCreateAttachment(AccessLog.class);
  }

  /**
   * Returns the main event of this task. Internally a task may generate multiple sub-events and send them through the pipeline. For example
   * a {@link ModifyFeaturesEvent} requires a {@link LoadFeaturesEvent} pre-flight event, some other events may require other pre-flight
   * request.
   *
   * @return the main event of this task.
   */
  public @NotNull EVENT event() {
    return event;
  }

  /**
   * Returns the request matrix for the current task state.
   *
   * @return The request matrix for the current task state.
   */
  public @NotNull XyzHubActionMatrix requestMatrix() {
    return requestMatrix;
  }

  /**
   * Send an unauthorized internal event through the pipeline.
   *
   * @param event the event to send.
   * @return the generated response.
   * @throws IllegalStateException if the pipeline is already in use.
   */
  protected final @NotNull XyzResponse sendUnauthorizedEvent(@NotNull Event event) {
    logger().info("Send unauthorized event " + event.getClass().getSimpleName());
    event.setStartNanos(startNanos());
    event.setStreamId(streamId);
    return pipeline.sendEvent(event);
  }

  /**
   * Send and authorize an event against the {@link #requestMatrix}.
   *
   * @param event the event to send.
   * @return the response.
   */
  protected final @NotNull XyzResponse sendAuthorizedEvent(@NotNull Event event) {
    return sendAuthorizedEvent(event, requestMatrix);
  }

  /**
   * Send and authorize an event.
   *
   * @param event       the event to send.
   * @param eventMatrix the request matrix that need to match the rights of the current user.
   * @return the response.
   */
  protected final @NotNull XyzResponse sendAuthorizedEvent(@NotNull Event event, @NotNull XyzHubActionMatrix eventMatrix) {
    logger().info("Send authorize event " + event.getClass().getSimpleName());
    event.setStartNanos(startNanos());
    event.setStreamId(streamId);
    final JWTPayload jwt = getJwt();
    if (jwt == null) {
      return errorResponse(XyzError.UNAUTHORIZED, "Accessing features isn't possible without a JWT token.");
    }
    // Note: The anonymous token must be configurable, we should not add any special handling for it!
    //    if (jwt.anonymous) {
    //      return errorResponse(XyzError.FORBIDDEN, "Accessing features isn't possible with an anonymous token.");
    //    }
    final ActionMatrix rightsMatrix = jwt.getXyzHubMatrix();
    if (rightsMatrix == null || !rightsMatrix.matches(eventMatrix)) {
      final String errorMessage = "Insufficient rights. " +
          "\nToken access: " + Json.encode(rightsMatrix) +
          "\nRequest access: " + Json.encode(eventMatrix);
      logger().info(errorMessage);
      return errorResponse(XyzError.FORBIDDEN, errorMessage);
    }
    if (logger().isDebugEnabled()) {
      logger().debug("Grant access.\nToken access: {}\nRequest access: {}", Json.encode(rightsMatrix), Json.encode(eventMatrix));
    }
    return pipeline.sendEvent(event);
  }

  /**
   * Send a response.
   *
   * @param response The response to send.
   */
  protected void sendResponse(@NotNull XyzResponse response) {
    assert routingContext != null;
    assert responseType != null;
    NakshaTask.rcSendResponse(routingContext, responseType, streamId, response);
  }

  /**
   * Send an error response for the given exception.
   *
   * @param routingContext The routing context for which to send the response.
   * @param responseType   The response type to return.
   * @param streamId       The stream-id.
   * @param throwable      The exception for which to send an error response.
   */
  public static void rcSendErrorResponse(
      @NotNull RoutingContext routingContext,
      @NotNull ApiResponseType responseType,
      @NotNull String streamId,
      @NotNull Throwable throwable
  ) {
    final ErrorResponse errorResponse;
    if (throwable instanceof XyzErrorException) {
      errorResponse = ((XyzErrorException) throwable).toErrorResponse(streamId);
    } else {
      errorResponse = new ErrorResponse();
      errorResponse.setStreamId(streamId);
      if (throwable instanceof ParameterError) {
        errorResponse.setError(XyzError.ILLEGAL_ARGUMENT);
      } else {
        errorResponse.setError(XyzError.EXCEPTION);
      }
      assert errorResponse.getError() != null;
      errorResponse.setErrorMessage(throwable.getMessage());
    }
    NakshaTask.rcSendResponse(routingContext, responseType, streamId, errorResponse);
  }

  /**
   * Send a response.
   *
   * @param routingContext The routing context for which to send the response.
   * @param responseType   The response type to return.
   * @param streamId       The stream-id.
   * @param response       The response to send.
   */
  public static void rcSendResponse(
      @NotNull RoutingContext routingContext,
      @NotNull ApiResponseType responseType,
      @NotNull String streamId,
      @NotNull XyzResponse response
  ) {
    try {
      final Map<@NotNull String, @NotNull String> headers;
      final String etag = response.getEtag();
      if (etag != null) {
        headers = stringMap("ETag", etag);
      } else {
        headers = null;
      }
      if (response instanceof ErrorResponse) {
        rcSendRawResponse(routingContext, streamId, OK, headers, APPLICATION_JSON, Buffer.buffer(response.serialize()));
        return;
      }
      if (response instanceof BinaryResponse br) {
        rcSendRawResponse(routingContext, streamId, OK, headers, br.getMimeType(), Buffer.buffer(br.getBytes()));
        return;
      }
      if (response instanceof NotModifiedResponse) {
        rcSendEmptyResponse(routingContext, streamId, NOT_MODIFIED, headers);
        return;
      }
      if (response instanceof FeatureCollection fc && responseType == ApiResponseType.FEATURE) {
        // If we should only send back a single feature.
        final List<@NotNull Feature> features = fc.getFeatures();
        if (features.size() == 0) {
          rcSendEmptyResponse(routingContext, streamId, OK, headers);
        } else {
          final String content = features.get(0).serialize();
          rcSendRawResponse(routingContext, streamId, OK, headers, responseType, Buffer.buffer(content));
        }
      }
      if (responseType == ApiResponseType.EMPTY) {
        rcSendEmptyResponse(routingContext, streamId, OK, headers);
        return;
      }
      rcSendRawResponse(routingContext, streamId, OK, headers, responseType, Buffer.buffer(response.serialize()));
    } catch (Throwable t) {
      currentLogger(routingContext).error("Unexpected failure while serializing response", t);
      rcSendFatalError(routingContext, streamId, t.getMessage());
    }
  }

  private static final Pattern FATAL_ERROR_MSG_PATTERN = Pattern.compile("^[0-9a-zA-Z .-_]+$");

  /**
   * Send back a fatal error, type {@link XyzError#EXCEPTION}.
   *
   * @param routingContext The routing context to send the response to.
   * @param streamId       The stream-id to return.
   * @param errorMessage   The error message to return.
   */
  private static void rcSendFatalError(
      @NotNull RoutingContext routingContext,
      @NotNull String streamId,
      @NotNull String errorMessage
  ) {
    assert FATAL_ERROR_MSG_PATTERN.matcher(errorMessage).matches();
    final String content = "{\n"
        + "\"type\": \"ErrorResponse\",\n"
        + "\"error\": \"Exception\",\n"
        + "\"errorMessage\": \"" + errorMessage + "\",\n"
        + "\"streamId\": \"" + streamId + "\"\n"
        + "}";
    rcSendRawResponse(routingContext, streamId, OK, null, APPLICATION_JSON, Buffer.buffer(content));
  }

  /**
   * Internal method to send back a response. The default content type will be {@code application/json}, except overridden via headers.
   *
   * @param routingContext The routing context to send the response to.
   * @param streamId       The stream-id to return.
   * @param status         The HTTP status code to set.
   * @param headers        The additional HTTP headers to set; if any.
   */
  private static void rcSendEmptyResponse(
      @NotNull RoutingContext routingContext,
      @NotNull String streamId,
      @NotNull HttpResponseStatus status,
      @Nullable Map<@NotNull String, @NotNull String> headers
  ) {
    rcSendRawResponse(routingContext, streamId, status, headers, null, null);
  }

  /**
   * Internal method to send back a response. The default content type will be {@code application/json}, except overridden via headers.
   *
   * @param routingContext The routing context to send the response to.
   * @param streamId       The stream-id to return.
   * @param status         The HTTP status code to set.
   * @param headers        The additional HTTP headers to set; if any.
   * @param contentType    The content-type; if any.
   * @param content        The content; if any.
   */
  private static void rcSendRawResponse(
      @NotNull RoutingContext routingContext,
      @NotNull String streamId,
      @NotNull HttpResponseStatus status,
      @Nullable Map<@NotNull String, @NotNull String> headers,
      @Nullable CharSequence contentType,
      @Nullable Buffer content
  ) {
    final HttpServerResponse httpResponse = routingContext.response();
    httpResponse.setStatusCode(status.code()).setStatusMessage(status.reasonPhrase());
    httpResponse.putHeader(STREAM_ID, streamId);
    if (headers != null) {
      for (final Map.Entry<@NotNull String, @NotNull String> entry : headers.entrySet()) {
        httpResponse.putHeader(entry.getKey(), entry.getValue());
      }
    }
    if (content == null || content.length() == 0) {
      httpResponse.end();
    } else {
      if (headers == null || !headers.containsKey(CONTENT_TYPE.toString())) {
        if (contentType == null) {
          httpResponse.putHeader(CONTENT_TYPE, APPLICATION_JSON);
        } else {
          httpResponse.putHeader(CONTENT_TYPE, contentType);
        }
      }
      assert httpResponse.headers().get(CONTENT_TYPE.toString()) != null;
      httpResponse.end(content);
    }
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
  protected static @NotNull Map<@NotNull String, @NotNull String> stringMap(@NotNull CharSequence... keyValueList) {
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