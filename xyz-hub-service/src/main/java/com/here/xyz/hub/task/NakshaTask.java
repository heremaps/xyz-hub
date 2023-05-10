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

import static com.here.xyz.hub.NakshaRoutingContext.routingContextLogger;

import com.here.xyz.AbstractTask;
import com.here.xyz.INaksha;
import com.here.xyz.NakshaLogger;
import com.here.xyz.events.Event;
import com.here.xyz.exceptions.ParameterError;
import com.here.xyz.hub.NakshaRoutingContext;
import com.here.xyz.hub.auth.ActionMatrix;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.params.XyzHubQueryParameters;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.util.logging.AccessLog;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.JsonUtils;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.MIMEHeader;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The XYZ-Hub task is the base of all tasks.
 *
 * @param <EVENT> The event type to execute.
 */
@SuppressWarnings({"SameParameterValue", "unused"})
public abstract class NakshaTask<EVENT extends Event> extends AbstractTask<EVENT> {

  /**
   * The key used in the routing context to attach a task to a routing context.
   */
  private static final String ROUTING_CONTEXT_NAKSHA_TASK_KEY = "nakshaTask";

  /**
   * Tries to return the Naksha task bound to the given routing context.
   *
   * @param routingContext The routing context to query.
   * @return The Naksha task attached or {@code null}, if no Naksha tasks attached.
   */
  public static @Nullable NakshaTask<?> get(@NotNull RoutingContext routingContext) {
    final Object raw = routingContext.get(ROUTING_CONTEXT_NAKSHA_TASK_KEY);
    //noinspection rawtypes
    return raw instanceof NakshaTask task ? task : null;
  }

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
   * The request matrix, by default an empty matrix that means no rights required to execute the task.
   */
  protected @NotNull XyzHubActionMatrix requestMatrix;

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
   * @param eventClass     The class of the event-type to execute.
   * @param routingContext The routing context.
   * @param responseTypes  The response types.
   * @param <E>            The event-type.
   */
  public static <E extends Event> void start(
      @NotNull Class<E> eventClass,
      @NotNull RoutingContext routingContext,
      @NotNull ApiResponseType... responseTypes) {
    final ApiResponseType apiResponseType = extractResponseType(routingContext, responseTypes);
    NakshaLogger logger = null;
    try {
      final NakshaTask<E> task = INaksha.instance.get().newTask(eventClass);
      task.routingContext = routingContext;
      task.responseType = apiResponseType;
      task.addListener(task::sendResponse);
      try {
        task.start();
      } catch (Throwable t) {
        task.sendResponse(task.errorResponse(t));
      }
    } catch (Throwable t) {
      routingContextLogger(routingContext).error("Failed to create task: {}", eventClass.getName(), t);
      NakshaRoutingContext.sendFatalErrorResponse(routingContext, "Failed to create task: " + eventClass.getSimpleName());
    }
  }

  protected NakshaTask(@NotNull EVENT event) {
    super(event);
    this.requestMatrix = new XyzHubActionMatrix();
  }

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
    routingContext.put(ROUTING_CONTEXT_NAKSHA_TASK_KEY, this);

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
   * Returns the request matrix for the current task state.
   *
   * @return The request matrix for the current task state.
   */
  public @NotNull XyzHubActionMatrix requestMatrix() {
    return requestMatrix;
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
  private void sendResponse(@NotNull XyzResponse response) {
    assert routingContext != null;
    assert responseType != null;
    NakshaRoutingContext.sendXyzResponse(routingContext, responseType, response);
  }

}