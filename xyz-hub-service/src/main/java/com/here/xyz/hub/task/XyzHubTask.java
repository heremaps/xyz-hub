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

import com.here.xyz.EventTask;
import com.here.xyz.events.Event;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.hub.auth.ActionMatrix;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.auth.XyzHubActionMatrix;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.util.logging.AccessLog;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import com.here.xyz.util.JsonUtils;
import io.vertx.core.json.Json;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.ext.auth.User;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A task representing a pipeline to process an action.
 */
public abstract class XyzHubTask extends EventTask {

  public static <T extends XyzHubTask> void startTask(
      @NotNull Class<T> taskClass,
      @NotNull RoutingContext routingContext,
      @NotNull ApiResponseType responseType
  ) {
    try {
      final T task = taskClass.getConstructor(RoutingContext.class, ApiResponseType.class).newInstance(routingContext, responseType);
      task.start();
    } catch (Throwable t) {
      String streamId = extractStreamIdFromRoutingContext(routingContext);
      if (streamId == null) {
        streamId = RandomStringUtils.randomAlphanumeric(12);
      }
      final ErrorResponse errorResponse;
      if (t instanceof XyzErrorException) {
        errorResponse = ((XyzErrorException) t).toErrorResponse(streamId);;
      } else {
        errorResponse = new ErrorResponse()
            .withStreamId(streamId)
            .withError(XyzError.EXCEPTION)
            .withErrorMessage(t.getMessage());
      }
      sendVertxResponse(routingContext, responseType, errorResponse);
    }
  }

  private static final Pattern STREAM_ID_PATTERN = Pattern.compile("^[a-zA-Z0-9_-]{10,}$");
  private static final Pattern IF_NONE_MATCH_PATTERN = Pattern.compile("^[a-zA-Z0-9/_-]{10,}$");

  /**
   * The corresponding routing context.
   */
  protected final @NotNull RoutingContext routingContext;

  /**
   * The response type that should be produced by this task.
   */
  protected @NotNull ApiResponseType responseType;

  private static @Nullable String extractStreamIdFromRoutingContext(@NotNull RoutingContext routingContext) {
    final String externalStreamId = routingContext.request().headers().get("Stream-Id");
    if (externalStreamId != null) {
      final Matcher matcher = STREAM_ID_PATTERN.matcher(externalStreamId);
      if (matcher.matches()) {
        return externalStreamId;
      }
    }
    return null;
  }

  protected XyzHubTask(@NotNull RoutingContext routingContext, @NotNull ApiResponseType responseType) throws XyzErrorException {
    super(extractStreamIdFromRoutingContext(routingContext));
    this.routingContext = routingContext;
    this.responseType = responseType;
    final String externalStreamId = routingContext.request().headers().get("Stream-Id");
    if (externalStreamId != null) {
      //noinspection StringEquality
      if (streamId != externalStreamId) {
        // Note: We can compare by reference, we need to have always the same reference.
        info("The externally provided 'Stream-Id' contains illegal characters, ignoring it: {}", externalStreamId);
      } else {
        //noinspection ConstantConditions
        assert !streamId.equals(externalStreamId);
        // Note: It must not be possible that the external stream ID is another reference than stream-id and both are equal!
      }
    }

    final String ifNoneMatch = routingContext.request().headers().get("If-None-Match");
    if (ifNoneMatch != null) {
      final Matcher matcher = IF_NONE_MATCH_PATTERN.matcher(ifNoneMatch);
      if (matcher.matches()) {
        this.ifNoneMatches = ifNoneMatch;
      } else {
        info("The externally provided 'If-None-Match' contains illegal characters, ignoring it: {}", ifNoneMatch);
      }
    }

    final User user = routingContext.user();
    if (user != null) {
      final JWTPayload jwt = DatabindCodec.mapper().convertValue(user.principal(), JWTPayload.class);
      if (jwt != null) {
        set(jwt);
        this.aid = jwt.aid;
        this.author = jwt.author;
      }
    }


  }

  /**
   * Returns the payload of the JWT Token.
   *
   * @return the payload of the JWT Token
   */
  public @Nullable JWTPayload getJwt() {
    return get(JWTPayload.class);
  }

  /**
   * Returns the access log object for this request.
   *
   * @return the access log object.
   */
  public @NotNull AccessLog accessLog() {
    return getOrCreate(AccessLog.class);
  }

  /**
   * The cached parsed query parameters.
   */
  private @Nullable XyzHubQueryParameters queryParams;

  /**
   * The If-None-Matches HTTP header value; if any given, see {@link Event#setIfNoneMatch(String)}.
   */
  protected @Nullable String ifNoneMatches;

  /**
   * The application-id to be set in the event, see {@link Event#setAid(String)}.
   */
  protected @Nullable String aid;

  /**
   * The author to be set in the event, see {@link Event#setAuthor(String)}.
   */
  protected @Nullable String author;

  /**
   * Returns the query parameters parsed by our self due to a bug in vertx, see: <a href="https://github.com/vert-x3/issues/issues/380>vertx
   * issue 380</a>
   *
   * @return The parsed query parameters.
   */
  public @NotNull XyzHubQueryParameters queryParams() {
    if (queryParams != null) {
      return queryParams;
    }
    final String query = routingContext.request().query();
    return this.queryParams = new XyzHubQueryParameters(query);
  }

  /**
   * Helper to initialize an event.
   *
   * @param event The event to initialize short before sending.
   * @param space The space to initialize the event.
   * @return the given event.
   */
  protected @NotNull Event initEvent(@NotNull Event event, @Nullable Space space) {
    event.setStreamId(streamId);
    event.setIfNoneMatch(ifNoneMatches);
    event.setAid(aid);
    event.setAuthor(author);
    event.setStartNanos(startNanos);
    // TODO: Add trusted parameters / Need to be extracted from routingContext!
    if (space != null) {
      event.setSpace(space.getId());
      event.setParams(JsonUtils.deepCopy(space.params));

      final List<@NotNull String> connectors = space.connectors;
      if (connectors != null && connectors.size() > 0) {
        final String storageConnectorId = connectors.get(connectors.size() - 1);
        final Connector storageConnector = Connector.getConnectorById(storageConnectorId);
        if (storageConnector != null) {
          event.setConnectorId(storageConnector.id);
          event.setConnectorNumber(storageConnector.number);
          event.setConnectorParams(JsonUtils.deepCopy(storageConnector.params));
        }
      }
    }
    final JWTPayload jwt = getJwt();
    if (jwt != null) {
      event.setMetadata(JsonUtils.deepCopy(jwt.metadata));
      // TODO: Clarify if we need to forward the token-id as all?
      //       We should not embed this into the event, it would be a security risk to do so!
      //       We should never trust any externally hosted connector!
      //event.setTid(jwt.tid);
    }
    return event;
  }

  /**
   * Send an unauthorized event through the pipeline, must only be called when the request is internal.
   *
   * @param event the event to send.
   * @return the generated response.
   * @throws IllegalStateException if the pipeline is already in use.
   */
  @Override
  public final @NotNull XyzResponse sendEvent(@NotNull Event event) {
    info("Send unauthorized event " + event.getClass().getSimpleName());
    // TODO: Add author and app into the event, so that our PSQL storage can invoke: naksha_tx_init(app, author)
    return super.sendEvent(event);
  }

  /**
   * Send and authorize an event. The event should have been {@link #initEvent(Event, Space) initialized} before sending.
   *
   * @param event       the event to send.
   * @param eventMatrix the request matrix that need to match the rights of the current user.
   * @return the response.
   */
  public final @NotNull XyzResponse sendEvent(@NotNull Event event, @NotNull XyzHubActionMatrix eventMatrix) {
    final JWTPayload jwt = getJwt();
    if (jwt == null) {
      return errorResponse(XyzError.UNAUTHORIZED, "Accessing features isn't possible without a JWT token.");
    }
    // Note: The anonymous token must be configurable, we should not add any special handling for it!
    //    if (jwt.anonymous) {
    //      return errorResponse(XyzError.FORBIDDEN, "Accessing features isn't possible with an anonymous token.");
    //    }
    final ActionMatrix rightsMatrix = jwt.getXyzHubMatrix();
    if (!rightsMatrix.matches(eventMatrix)) {
      final String errorMessage = "Insufficient rights. " +
          "\nToken access: " + Json.encode(rightsMatrix) +
          "\nRequest access: " + Json.encode(eventMatrix);
      info(errorMessage);
      return errorResponse(XyzError.FORBIDDEN, errorMessage);
    }
    if (isDebugEnabled()) {
      debug("Grant access.\nToken access: {}\nRequest access: {}", Json.encode(rightsMatrix), Json.encode(eventMatrix));
    }
    return super.sendEvent(event);
  }

  @Override
  final protected void sendResponse(@NotNull XyzResponse response) {
    final AccessLog accessLog = accessLog();
    // TODO: Log the response and fill the access log.
    sendVertxResponse(routingContext, responseType, response);
  }

  private static void sendVertxResponse(
      @NotNull RoutingContext routingContext,
      @NotNull ApiResponseType responseType,
      @NotNull XyzResponse response) {
    // TODO: Implement sending the response, serialized depending on the given response type!
    //       See Api for implementations!
  }
}
