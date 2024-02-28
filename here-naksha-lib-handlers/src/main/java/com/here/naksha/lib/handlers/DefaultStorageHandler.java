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
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.NOT_IMPLEMENTED;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.PROCESS;
import static com.here.naksha.lib.handlers.DefaultStorageHandler.OperationAttempt.ATTEMPT_AFTER_COLLECTION_CREATION;
import static com.here.naksha.lib.handlers.DefaultStorageHandler.OperationAttempt.ATTEMPT_AFTER_STORAGE_INITIALIZATION;
import static com.here.naksha.lib.handlers.DefaultStorageHandler.OperationAttempt.FIRST_ATTEMPT;
import static com.here.naksha.lib.psql.EPsqlState.COLLECTION_DOES_NOT_EXIST;
import static com.here.naksha.lib.psql.EPsqlState.UNDEFINED_TABLE;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.StorageNotInitialized;
import com.here.naksha.lib.core.lambdas.F1;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventTarget;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.naksha.SpaceProperties;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.WriteCollections;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodec;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.handlers.exceptions.MissingCollectionsException;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultStorageHandler extends AbstractEventHandler {

  private static final Logger logger = LoggerFactory.getLogger(DefaultStorageHandler.class);
  private static final Set<String> MISSING_COLLECTION_SQL_ERROR_STATES =
      Set.of(UNDEFINED_TABLE.toString(), COLLECTION_DOES_NOT_EXIST.toString());

  protected @NotNull EventHandler eventHandler;
  protected @NotNull EventTarget<?> eventTarget;
  protected @NotNull DefaultStorageHandlerProperties properties;

  public DefaultStorageHandler(
      final @NotNull EventHandler eventHandler,
      final @NotNull INaksha hub,
      final @NotNull EventTarget<?> eventTarget) {
    super(hub);
    this.eventHandler = eventHandler;
    this.eventTarget = eventTarget;
    this.properties = JsonSerializable.convert(eventHandler.getProperties(), DefaultStorageHandlerProperties.class);
  }

  @Override
  protected EventProcessingStrategy processingStrategyFor(IEvent event) {
    final Request<?> request = event.getRequest();
    if (request instanceof ReadFeatures
        || request instanceof WriteFeatures
        || request instanceof WriteCollections) {
      return PROCESS;
    }
    return NOT_IMPLEMENTED;
  }

  @Override
  public @NotNull Result process(@NotNull IEvent event) {
    final NakshaContext ctx = NakshaContext.currentContext();
    final Request<?> request = event.getRequest();

    logger.info("Handler received request {}", request.getClass().getSimpleName());
    // Obtain storageId from EventHandler object
    final String storageId = properties.getStorageId();
    if (storageId == null) {
      logger.error("No storageId configured");
      return new ErrorResult(XyzError.NOT_FOUND, "No storageId configured for handler.");
    }
    logger.info("Against Storage id={}", storageId);
    addStorageIdToStreamInfo(storageId, ctx);

    // Obtain IStorage implementation using NakshaHub
    final IStorage storageImpl = nakshaHub().getStorageById(storageId);
    logger.info("Using storage implementation [{}]", storageImpl.getClass().getName());

    XyzCollection collection = chooseCollection();
    applyCollectionId(request, collection.getId());
    return forwardRequestToStorage(ctx, request, storageImpl, collection, FIRST_ATTEMPT);
  }

  private @NotNull Result forwardRequestToStorage(
      final @NotNull NakshaContext ctx,
      final @NotNull Request<?> request,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection,
      final @NotNull OperationAttempt currentAttempt) {
    if (request instanceof ReadFeatures rf) {
      return forwardReadFeatures(ctx, storageImpl, collection, rf, currentAttempt);
    } else if (request instanceof WriteFeatures<?, ?, ?> wf) {
      return forwardWriteFeatures(ctx, storageImpl, collection, wf, currentAttempt);
    } else if (request instanceof WriteCollections<?, ?, ?> wc) {
      return forwardWriteCollections(ctx, storageImpl, collection, wc, currentAttempt);
    } else {
      return notImplemented(request);
    }
  }

  private @NotNull Result forwardReadFeatures(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection,
      final @NotNull ReadFeatures rf,
      final @NotNull OperationAttempt currentAttempt) {
    logger.info("Processing ReadFeatures against {}", collection.getId());
    try (final IReadSession reader = storageImpl.newReadSession(ctx, false)) {
      return reader.execute(rf);
    } catch (RuntimeException re) {
      return reattemptFeatureRequest(ctx, storageImpl, collection, rf, currentAttempt, re);
    }
  }

  private @NotNull Result forwardWriteFeatures(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection,
      final @NotNull WriteFeatures<?, ?, ?> wf,
      final OperationAttempt operationAttempt) {
    logger.info("Processing WriteFeatures against {}", collection.getId());
    return forwardWriteRequest(
        ctx,
        storageImpl,
        wf,
        re -> reattemptFeatureRequest(ctx, storageImpl, collection, wf, operationAttempt, re));
  }

  private @NotNull Result forwardWriteCollections(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection,
      final @NotNull WriteCollections<?, ?, ?> wc,
      final OperationAttempt operationAttempt) {
    logger.info("Processing WriteCollections against {}", collection.getId());
    if (isPurgeCollectionRequest(wc)) {
      if (properties.getAutoDeleteCollection()) {
        return forwardWriteRequest(
            ctx,
            storageImpl,
            wc,
            re -> reattemptCollectionRequest(ctx, storageImpl, collection, wc, operationAttempt, re));
      } else {
        logger.info(
            "Received delete collection request but autoDelete is not enabled, returning success without any action");
        return new SuccessResult();
      }
    } else {
      logger.info(
          "Handling WriteCollections only with single collection deletion, returning success without any action");
      return new SuccessResult();
    }
  }

  private boolean isPurgeCollectionRequest(@NotNull WriteCollections<?, ?, ?> wc) {
    return wc.features.size() == 1
        && EWriteOp.PURGE.toString().equals(wc.features.get(0).getOp());
  }

  private @NotNull Result forwardWriteRequest(
      @NotNull NakshaContext ctx,
      @NotNull IStorage storageImpl,
      @NotNull WriteRequest<?, ?, ?> wr,
      @NotNull F1<Result, RuntimeException> reattempt) {
    try (final IWriteSession writer = storageImpl.newWriteSession(ctx, true)) {
      final Result result = writer.execute(wr);
      if (result instanceof SuccessResult) {
        writer.commit(true);
      } else {
        logger.warn("Failed executing {}, expected success but got: {}", wr.getClass(), result);
        writer.rollback(true);
      }
      return result;
    } catch (RuntimeException re) {
      return reattempt.call(re);
    }
  }

  private @NotNull Result reattemptFeatureRequest(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection,
      final @NotNull Request<?> request,
      final @NotNull OperationAttempt previousAttempt,
      final @NotNull RuntimeException re) {
    return switch (previousAttempt) {
      case FIRST_ATTEMPT -> reattemptFeatureRequestForTheFirstTime(ctx, storageImpl, collection, request, re);
      case ATTEMPT_AFTER_STORAGE_INITIALIZATION -> reattemptAfterStorageInitialization(
          ctx, storageImpl, collection, request, re);
      case ATTEMPT_AFTER_COLLECTION_CREATION -> throw re;
    };
  }

  private @NotNull Result reattemptCollectionRequest(
      NakshaContext ctx,
      IStorage storageImpl,
      XyzCollection collection,
      WriteCollections<?, ?, ?> wc,
      OperationAttempt previousAttempt,
      RuntimeException re) {
    if (previousAttempt == FIRST_ATTEMPT && re instanceof StorageNotInitialized) {
      return retryDueToUninitializedStorage(ctx, storageImpl, collection, wc);
    }
    logger.warn(
        "No further reattempt strategy available for WriteCollections request (collectionId: {}, previous attempt: {}. Rethrowing original exception",
        collection.getId(),
        previousAttempt);
    throw re;
  }

  private @NotNull Result reattemptFeatureRequestForTheFirstTime(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection,
      final @NotNull Request<?> request,
      final @NotNull RuntimeException re) {
    if (re instanceof StorageNotInitialized) {
      return retryDueToUninitializedStorage(ctx, storageImpl, collection, request);
    } else if (indicatesMissingCollection(re)) {
      try {
        return retryDueToMissingCollection(ctx, storageImpl, collection, request);
      } catch (MissingCollectionsException mce) {
        logger.info("Retrying due to missing collection failed", mce);
        return mce.toErrorResult();
      }
    } else {
      throw re;
    }
  }

  private @NotNull Result reattemptAfterStorageInitialization(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection,
      final @NotNull Request<?> request,
      final @NotNull RuntimeException re) {
    if (indicatesMissingCollection(re)) {
      try {
        return retryDueToMissingCollection(ctx, storageImpl, collection, request);
      } catch (MissingCollectionsException mce) {
        logger.info("Retrying due to missing collection failed", mce);
        return mce.toErrorResult();
      }
    } else {
      throw re;
    }
  }

  private boolean indicatesMissingCollection(RuntimeException re) {
    if (re.getCause() instanceof SQLException sqe) {
      return MISSING_COLLECTION_SQL_ERROR_STATES.contains(sqe.getSQLState());
    }
    return false;
  }

  @NotNull
  private Result retryDueToUninitializedStorage(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection,
      final @NotNull Request<?> request) {
    logger.info("Initializing Storage before reattempting write request.");
    storageImpl.initStorage();
    logger.info("Storage initialized");
    return forwardRequestToStorage(ctx, request, storageImpl, collection, ATTEMPT_AFTER_STORAGE_INITIALIZATION);
  }

  private Result retryDueToMissingCollection(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection,
      final @NotNull Request<?> request) {
    logger.warn("Collection not found for {}", collection.getId());
    if (properties.getAutoCreateCollection()) {
      logger.info(
          "Collection auto creation is enabled, attempting to create collection specified in request: {}",
          collection.getId());
      createXyzCollection(ctx, storageImpl, collection);
      logger.info("Created collection {}, forwarding the request once again", collection.getId());
      return forwardRequestToStorage(ctx, request, storageImpl, collection, ATTEMPT_AFTER_COLLECTION_CREATION);
    } else {
      logger.warn(
          "Collection auto creation is disabled, failing due to missing collection specified in request: {}",
          collection.getId());
      throw new MissingCollectionsException(collection);
    }
  }

  private void applyCollectionId(Request<?> request, @NotNull String customCollectionId) {
    if (request instanceof ReadFeatures rf) {
      rf.setCollections(List.of(customCollectionId));
    } else if (request instanceof WriteFeatures<?, ?, ?> wf) {
      wf.setCollectionId(customCollectionId);
    } else if (request instanceof WriteCollections<?, ?, ?> wc) {
      collectionsFrom(wc).forEach(collection -> collection.setId(customCollectionId));
    }
  }

  // TODO: collectionId at handler level can be potentially removed in the future
  private @NotNull XyzCollection chooseCollection() {
    final XyzCollection collectionDefinedInHandler = properties.getXyzCollection();
    if (collectionDefinedInHandler != null) {
      logger.info(
          "Using collection with id {} that is associated with EventHandler(id={})",
          collectionDefinedInHandler.getId(),
          eventHandler.getId());
      return collectionDefinedInHandler;
    }
    if (eventTarget instanceof Space s) {
      final SpaceProperties spaceProperties = JsonSerializable.convert(s.getProperties(), SpaceProperties.class);
      final XyzCollection collectionDefinedInSpace = spaceProperties.getXyzCollection();
      if (collectionDefinedInSpace != null) {
        logger.info(
            "Using collection with id {} that is associated with Space(id={})",
            collectionDefinedInSpace.getId(),
            s.getId());
        return collectionDefinedInSpace;
      }
    }
    logger.info(
        "No collection definition found in Handler & Space properties, using default one with event target id: {}",
        eventTarget.getId());
    return new XyzCollection(eventTarget.getId());
  }

  private @NotNull Stream<@NotNull XyzCollection> collectionsFrom(@NotNull WriteCollections<?, ?, ?> wc) {
    return wc.features.stream()
        .filter(XyzCollectionCodec.class::isInstance)
        .map(XyzCollectionCodec.class::cast)
        .map(XyzCollectionCodec::getFeature)
        .filter(Objects::nonNull);
  }

  private void createXyzCollection(
      final @NotNull NakshaContext ctx,
      final @NotNull IStorage storageImpl,
      final @NotNull XyzCollection collection) {
    try (final IWriteSession writer = storageImpl.newWriteSession(ctx, true)) {
      final Result result = writer.execute(createWriteCollectionsRequest(collection));
      if (result instanceof SuccessResult) {
        writer.commit(true);
      } else {
        logger.error(
            "Unexpected result while creating collection {}. Result - {}. Executing rollback",
            collection.getId(),
            result);
        writer.rollback(true);
        throw unchecked(new Exception("Failed creating collection " + collection.getId()));
      }
    }
  }

  enum OperationAttempt {
    FIRST_ATTEMPT,
    ATTEMPT_AFTER_STORAGE_INITIALIZATION,
    ATTEMPT_AFTER_COLLECTION_CREATION
  }
}
