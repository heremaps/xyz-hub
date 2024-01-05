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
package com.here.naksha.lib.hub.storages;

import static com.here.naksha.lib.core.util.storage.RequestHelper.readFeaturesByIdRequest;
import static com.here.naksha.lib.core.util.storage.RequestHelper.readFeaturesByIdsRequest;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeatureFromResult;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesFromResult;

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.ReadCollections;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.util.StreamInfo;
import com.here.naksha.lib.handlers.AuthorizationEventHandler;
import com.here.naksha.lib.hub.EventPipelineFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NHSpaceStorageReader implements IReadSession {

  private static final int DEFAULT_FETCH_SIZE = 1_000;
  private static final Logger logger = LoggerFactory.getLogger(NHSpaceStorageReader.class);

  /** Singleton instance of NakshaHub storage implementation */
  protected final @NotNull INaksha nakshaHub;
  /** Runtime NakshaContext which is to be associated with read operations */
  protected final @NotNull NakshaContext context;
  /** Flag to indicate whether it has to connect to master storage instance or not */
  protected final boolean useMaster;
  /** List of Admin virtual spaces with relevant event handlers required to support event processing */
  protected final @NotNull Map<String, List<IEventHandler>> virtualSpaces;

  protected final @NotNull EventPipelineFactory pipelineFactory;

  private @NotNull int fetchSize;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public NHSpaceStorageReader(
      final @NotNull INaksha hub,
      final @NotNull Map<String, List<IEventHandler>> virtualSpaces,
      final @NotNull EventPipelineFactory pipelineFactory,
      final @Nullable NakshaContext context,
      boolean useMaster) {
    this.nakshaHub = hub;
    this.virtualSpaces = virtualSpaces;
    this.pipelineFactory = pipelineFactory;
    this.context = (context != null) ? context : NakshaContext.currentContext();
    this.useMaster = useMaster;
    fetchSize = DEFAULT_FETCH_SIZE;
  }

  /**
   * Tests whether this session is connected to the master-node.
   *
   * @return {@code true}, if this session is connected to the master-node; {@code false} otherwise.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public boolean isMasterConnect() {
    return useMaster;
  }

  /**
   * Returns the Naksha context bound to this read-connection.
   *
   * @return the Naksha context bound to this read-connection.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull NakshaContext getNakshaContext() {
    return this.context;
  }

  /**
   * Returns the amount of features to fetch at ones.
   *
   * @return the amount of features to fetch at ones.
   */
  @Override
  public int getFetchSize() {
    return fetchSize;
  }

  /**
   * Changes the amount of features to fetch at ones.
   *
   * @param size The amount of features to fetch at ones.
   */
  @Override
  public void setFetchSize(int size) {
    this.fetchSize = size;
  }

  /**
   * Returns the statement timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  /**
   * Sets the statement timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) {}

  /**
   * Returns the lock timeout.
   *
   * @param timeUnit The time-unit in which to return the timeout.
   * @return The timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return 0;
  }

  /**
   * Sets the lock timeout.
   *
   * @param timeout  The timeout to set.
   * @param timeUnit The unit of the timeout.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) {}

  /**
   * Execute the given read-request.
   *
   * @param readRequest input read request
   * @return the result.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull Result execute(final @NotNull ReadRequest<?> readRequest) {
    if (readRequest instanceof ReadCollections rc) {
      return executeReadCollections(rc);
    } else if (readRequest instanceof ReadFeatures rf) {
      return executeReadFeatures(rf);
    }
    throw new UnsupportedOperationException(
        "ReadRequest with unsupported type " + readRequest.getClass().getName());
  }

  private @NotNull Result executeReadCollections(final @NotNull ReadCollections rc) {
    logger.info("ReadCollections Request for {}, against Admin storage.", rc.getIds());
    try (final IReadSession admin = nakshaHub.getAdminStorage().newReadSession(context, useMaster)) {
      return admin.execute(rc);
    }
  }

  private @NotNull Result executeReadFeatures(final @NotNull ReadFeatures rf) {
    if (rf.getCollections().size() > 1) {
      throw new UnsupportedOperationException("Reading from multiple spaces not supported!");
    }
    final String spaceId = rf.getCollections().get(0);
    logger.info("ReadFeatures Request against spaceId={}", spaceId);
    addSpaceIdToStreamInfo(spaceId);
    if (virtualSpaces.containsKey(spaceId)) {
      // Request is to read from Naksha Admin space
      return executeReadFeaturesFromAdminSpaces(rf);
    } else {
      // Request is to read from Custom space
      return executeReadFeaturesFromCustomSpaces(rf);
    }
  }

  private @NotNull Result executeReadFeaturesFromAdminSpaces(final @NotNull ReadFeatures rf) {
    // Run pipeline against virtual space
    final String spaceId = rf.getCollections().get(0);
    final EventPipeline pipeline = pipelineFactory.eventPipeline();
    final Result result = setupEventPipelineForAdminVirtualSpace(spaceId, pipeline);
    if (!(result instanceof SuccessResult)) return result;
    return pipeline.sendEvent(rf);
  }

  protected @NotNull Result setupEventPipelineForAdminVirtualSpace(
      final @NotNull String spaceId, final @NotNull EventPipeline pipeline) {
    // add internal Admin resource specific event handlers
    final StringBuilder handlerTypes = new StringBuilder();
    for (final IEventHandler handler : virtualSpaces.get(spaceId)) {
      pipeline.addEventHandler(handler);
      if (handlerTypes.length() == 0) {
        handlerTypes.append(handler.getClass().getSimpleName());
      } else {
        handlerTypes.append(",").append(handler.getClass().getSimpleName());
      }
    }
    logger.info("Handler types identified [{}]", handlerTypes);
    return new SuccessResult();
  }

  private @NotNull Result executeReadFeaturesFromCustomSpaces(final @NotNull ReadFeatures rf) {
    if (rf.getCollections().size() > 1) {
      return new ErrorResult(
          XyzError.NOT_IMPLEMENTED, "ReadFeatures from multiple collections not supported at present!");
    }
    final String spaceId = rf.getCollections().get(0);
    final EventPipeline eventPipeline = pipelineFactory.eventPipeline();
    final Result result = setupEventPipelineForSpaceId(spaceId, eventPipeline);
    if (!(result instanceof SuccessResult)) return result;
    return eventPipeline.sendEvent(rf);
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  protected @NotNull Result setupEventPipelineForSpaceId(
      final @NotNull String spaceId, final @NotNull EventPipeline pipeline) {
    Space space = null;
    List<EventHandler> eventHandlers = null;

    try (final IReadSession reader = nakshaHub.getAdminStorage().newReadSession(context, false)) {
      // Get Space details using Admin Storage
      Result result = reader.execute(readFeaturesByIdRequest(NakshaAdminCollection.SPACES, spaceId));
      if (result instanceof ErrorResult er) {
        return er;
      } else {
        space = readFeatureFromResult(result, Space.class);
      }
      if (space == null) {
        return new ErrorResult(XyzError.NOT_FOUND, "Space not found : " + spaceId);
      }
      if (space.getEventHandlerIds() == null || space.getEventHandlerIds().isEmpty()) {
        return new ErrorResult(XyzError.NOT_FOUND, "No associated handler");
      }

      logger.info("Handler IDs identified {}", space.getEventHandlerIds());
      // Get EventHandler Details using Admin Storage
      result = reader.execute(
          readFeaturesByIdsRequest(NakshaAdminCollection.EVENT_HANDLERS, space.getEventHandlerIds()));
      if (result instanceof ErrorResult er) {
        return er;
      } else {
        try {
          eventHandlers = readFeaturesFromResult(result, EventHandler.class);
          if (eventHandlers.size() != space.getEventHandlerIds().size()) {
            return new ErrorResult(
                XyzError.EXCEPTION, "Not all EventHandlers found for space : " + spaceId);
          }
        } catch (NoCursor | NoSuchElementException e) {
          return new ErrorResult(XyzError.EXCEPTION, "No handlers associated with space : " + spaceId);
        }
      }
    }

    // Instantiate IEventHandler (from EventHandler object), using NakshaHub and Space details
    final List<IEventHandler> handlerImpls = new ArrayList<>();
    for (final EventHandler eventHandler : eventHandlers) {
      if (!eventHandler.isActive()) {
        logger.warn("Skipping inactive event handler {}", eventHandler.getId());
        continue;
      }
      handlerImpls.add(eventHandler.newInstance(nakshaHub, space));
    }
    if (handlerImpls.isEmpty()) {
      return new ErrorResult(XyzError.EXCEPTION, "No active EventHandlers found for space : " + spaceId);
    }

    // Create pipeline and add all applicable event handlers
    // TODO : AuthorizationHandler will need information about Space storageId as well
    pipeline.addEventHandler(new AuthorizationEventHandler(nakshaHub, space, eventHandlers));
    final StringBuilder handlerTypes = new StringBuilder();
    for (final IEventHandler handler : handlerImpls) {
      pipeline.addEventHandler(handler);
      if (handlerTypes.length() == 0) {
        handlerTypes.append(handler.getClass().getSimpleName());
      } else {
        handlerTypes.append(",").append(handler.getClass().getSimpleName());
      }
    }
    logger.info("Handler types identified [{}]", handlerTypes);
    return new SuccessResult();
  }

  /**
   * Process the given notification.
   *
   * @param notification notification event
   * @return the result.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull Result process(@NotNull Notification<?> notification) {
    throw new UnsupportedOperationException("Notification processing not supported!");
  }

  /**
   * Closes the session, returns the underlying connection back to the connection pool. Any method of the session will from now on throw an
   * {@link IllegalStateException}.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void close() {}

  protected void addSpaceIdToStreamInfo(final @Nullable String spaceId) {
    final StreamInfo streamInfo = context.getStreamInfo();
    if (streamInfo != null) streamInfo.setSpaceIdIfMissing(spaceId);
  }
}
