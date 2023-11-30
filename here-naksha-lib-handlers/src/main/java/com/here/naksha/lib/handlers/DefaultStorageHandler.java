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
package com.here.naksha.lib.handlers;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.util.storage.RequestHelper.createWriteCollectionsRequest;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventHandlerProperties;
import com.here.naksha.lib.core.models.naksha.EventTarget;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.naksha.SpaceProperties;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.psql.EPsqlState;
import java.sql.SQLException;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStorageHandler extends AbstractEventHandler {

  private static final Logger logger = LoggerFactory.getLogger(DefaultStorageHandler.class);
  protected @NotNull EventHandler eventHandler;
  protected @NotNull EventTarget<?> eventTarget;
  protected @NotNull EventHandlerProperties properties;

  public DefaultStorageHandler(
      final @NotNull EventHandler eventHandler,
      final @NotNull INaksha hub,
      final @NotNull EventTarget<?> eventTarget) {
    super(hub);
    this.eventHandler = eventHandler;
    this.eventTarget = eventTarget;
    this.properties = JsonSerializable.convert(eventHandler.getProperties(), EventHandlerProperties.class);
  }

  /**
   * The method invoked by the event-pipeline to process custom Storage specific read/write operations
   *
   * @param event the event to process.
   * @return the result.
   */
  @Override
  public @NotNull Result processEvent(@NotNull IEvent event) {
    final NakshaContext ctx = NakshaContext.currentContext();
    final Request<?> request = event.getRequest();

    logger.info("Handler received request {}", request.getClass().getSimpleName());
    // Obtain storageId from EventHandler object
    final String storageId = properties.getStorageId();
    if (storageId == null) {
      logger.error("No storageId configured");
      return new ErrorResult(XyzError.NOT_FOUND, "No storageId configured for handler.");
    }
    logger.info("Adding storage id ('{}') to stream", storageId); // TODO: delete me
    addStorageIdToStreamInfo(storageId, ctx);

    // Obtain IStorage implementation using NakshaHub
    logger.info("Fetching storage impl by id ('{}')", storageId); // TODO: delete me
    final IStorage storageImpl = nakshaHub().getStorageById(storageId);

    // Find collectionId from EventHandler, from Space, whichever is available first
    logger.info("Fetched storage impl by id ('{}')", storageId); // TODO: delete me
    String customCollectionId = null;
    if (properties.getStorageCollection() != null) {
      customCollectionId = properties.getStorageCollection().getId();
      logger.info("Using collectionId {} associated with EventHandler", customCollectionId);
    }
    if (customCollectionId == null && eventTarget instanceof Space s) {
      logger.info("Custom collection is null, getting space props"); // TODO: delete me
      final SpaceProperties spaceProperties = JsonSerializable.convert(s.getProperties(), SpaceProperties.class);
      logger.info("Fetched space props"); // TODO: delete me
      if (spaceProperties.getStorageCollection() != null) {
        customCollectionId = spaceProperties.getStorageCollection().getId();
        logger.info("Using collectionId {} associated with Space", customCollectionId);
      }
    }

    // Trigger respective read/write operation on respective IStorage implementation
    return forwardRequestToStorage(event, ctx, request, storageImpl, customCollectionId);
  }

  private @NotNull Result forwardRequestToStorage(
      final @NotNull IEvent event,
      final @NotNull NakshaContext ctx,
      final @NotNull Request<?> request,
      final @NotNull IStorage storageImpl,
      final @Nullable String customCollectionId) {
    if (request instanceof ReadFeatures rf) {
      return forwardReadFeaturesToStorage(ctx, storageImpl, customCollectionId, rf, false);
    } else if (request instanceof WriteFeatures<?, ?, ?> wf) {
      return forwardWriteFeaturesToStorage(ctx, storageImpl, customCollectionId, wf, false);
    } else {
      return notImplemented(event);
    }
  }

  private @NotNull Result forwardReadFeaturesToStorage(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @Nullable String customCollectionId,
      final @NotNull ReadFeatures rf,
      final boolean isReattempt) {
    // overwrite collectionId with custom one if available
    if (customCollectionId != null) {
      rf.setCollections(List.of(customCollectionId));
    }
    logger.info("Processing ReadFeatures against {}", rf.getCollections());
    try (final IReadSession reader = storageImpl.newReadSession(ctx, false)) {
      return reader.execute(rf);
    } catch (RuntimeException re) {
      if (!isReattempt && re.getCause() instanceof SQLException sqe) {
        // if it was "table not found" exception, then creation collection and reattempt the request
        if (EPsqlState.UNDEFINED_TABLE.toString().equals(sqe.getSQLState())) {
          logger.warn(
              "Collection not found for {}, so we will attempt collection creation and then reattempt read request.",
              rf.getCollections());
          createStorageCollections(ctx, storageImpl, rf.getCollections());
          return forwardReadFeaturesToStorage(ctx, storageImpl, customCollectionId, rf, true);
        } else {
          throw re;
        }
      } else {
        throw re;
      }
    }
  }

  private @NotNull Result forwardWriteFeaturesToStorage(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @Nullable String customCollectionId,
      final @NotNull WriteFeatures<?, ?, ?> wf,
      final boolean isReattempt) {
    // overwrite collectionId with custom one if available
    if (customCollectionId != null) {
      wf.setCollectionId(customCollectionId);
    }
    logger.info("Processing WriteFeatures against {}", wf.getCollectionId());
    try (final IWriteSession writer = storageImpl.newWriteSession(ctx, true)) {
      final Result result = writer.execute(wf);
      if (result instanceof SuccessResult) {
        writer.commit(true);
      }
      return result;
    } catch (RuntimeException re) {
      if (!isReattempt && re.getCause() instanceof SQLException sqe) {
        // if it was "table not found" exception, then creation collection and reattempt the request
        if (EPsqlState.UNDEFINED_TABLE.toString().equals(sqe.getSQLState())) {
          logger.warn(
              "Collection not found for {}, so we will attempt collection creation and then reattempt write request.",
              wf.getCollectionId());
          createStorageCollections(ctx, storageImpl, List.of(wf.getCollectionId()));
          return forwardWriteFeaturesToStorage(ctx, storageImpl, customCollectionId, wf, true);
        } else {
          throw re;
        }
      } else {
        throw re;
      }
    }
  }

  private void createStorageCollections(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull List<String> collectionIds) {
    try (final IWriteSession writer = storageImpl.newWriteSession(ctx, true)) {
      final Result result = writer.execute(createWriteCollectionsRequest(collectionIds));
      if (result instanceof SuccessResult) {
        writer.commit(true);
      } else {
        logger.error("Unexpected result while creating collection {}. Result - {}", collectionIds, result);
        throw unchecked(new Exception("Failed creating collection " + collectionIds));
      }
    }
  }
}
