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
package com.here.naksha.lib.handlers;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.StorageNotInitialized;
import com.here.naksha.lib.core.lambdas.F1;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.*;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.StreamInfo;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.handlers.exceptions.MissingCollectionsException;
import org.apache.commons.lang3.time.StopWatch;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.NOT_IMPLEMENTED;
import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.PROCESS;
import static com.here.naksha.lib.handlers.DefaultStorageHandler.OperationAttempt.*;
import static com.here.naksha.lib.psql.EPsqlState.COLLECTION_DOES_NOT_EXIST;
import static com.here.naksha.lib.psql.EPsqlState.UNDEFINED_TABLE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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

    XyzCollection collection = chooseCollection(request);
    applyCollectionId(request, collection.getId());

    StopWatch storageTimer = new StopWatch();
    try {
      return forwardRequestToStorage(ctx, request, storageImpl, collection, FIRST_ATTEMPT, storageTimer);
    } finally {
      addStorageTimeToStreamInfo(storageTimer, ctx);
    }
  }

  private void addStorageTimeToStreamInfo(StopWatch storageTimer, NakshaContext ctx) {
    StreamInfo streamInfo = ctx.getStreamInfo();
    if (streamInfo != null) {
      streamInfo.increaseTimeInStorage(NANOSECONDS.toMillis(storageTimer.getNanoTime()));
    }
  }

  private <T> T measuredStorageSupplier(Supplier<T> operation, StopWatch stopWatch) {
    try {
      if (stopWatch.isSuspended()) {
        stopWatch.resume();
      } else {
        stopWatch.start();
      }
      return operation.get();
    } finally {
      stopWatch.suspend();
    }
  }

  private void measuredStorageRunnable(Runnable operation, StopWatch stopWatch) {
    try {
      if (stopWatch.isSuspended()) {
        stopWatch.resume();
      } else {
        stopWatch.start();
      }
      operation.run();
    } finally {
      stopWatch.suspend();
    }
  }

  private @NotNull Result forwardRequestToStorage(
          final @NotNull NakshaContext ctx,
          final @NotNull Request<?> request,
          final @NotNull IStorage storageImpl,
          final @NotNull XyzCollection collection,
          final @NotNull OperationAttempt currentAttempt,
          final @NotNull StopWatch storageTimer) {
    if (request instanceof ReadFeatures rf) {
      return forwardReadFeatures(ctx, storageImpl, collection, rf, currentAttempt, storageTimer);
    } else if (request instanceof WriteFeatures<?, ?, ?> wf) {
      return forwardWriteFeatures(ctx, storageImpl, collection, wf, currentAttempt, storageTimer);
    } else if (request instanceof WriteCollections<?, ?, ?> wc) {
      return forwardWriteCollections(ctx, storageImpl, collection, wc, currentAttempt, storageTimer);
    } else {
      return notImplemented(request);
    }
  }

  private @NotNull Result forwardReadFeatures(
          final @NotNull NakshaContext ctx,
          final @NotNull IStorage storageImpl,
          final @NotNull XyzCollection collection,
          final @NotNull ReadFeatures rf,
          final @NotNull OperationAttempt currentAttempt,
          final @NotNull StopWatch storageTimer) {
    logger.info("Processing ReadFeatures against {}", collection.getId());
    try {
      return measuredStorageSupplier(() -> singleRead(ctx, storageImpl, rf), storageTimer);
    } catch (RuntimeException re) {
      return reattemptFeatureRequest(ctx, storageImpl, collection, rf, currentAttempt, re, storageTimer);
    }
  }

  private @NotNull Result singleRead(
          final @NotNull NakshaContext ctx, final @NotNull IStorage storageImpl, final @NotNull ReadFeatures rf) {
    try (final IReadSession reader = storageImpl.newReadSession(ctx, false)) {
      return reader.execute(rf);
    }
  }

  private @NotNull Result forwardWriteFeatures(
          final @NotNull NakshaContext ctx,
          final @NotNull IStorage storageImpl,
          final @NotNull XyzCollection collection,
          final @NotNull WriteFeatures<?, ?, ?> wf,
          final OperationAttempt operationAttempt,
          final @NotNull StopWatch storageTimer) {
    logger.info("Processing WriteFeatures against {}", collection.getId());
    return forwardWriteRequest(
            ctx,
            storageImpl,
            wf,
            re -> reattemptFeatureRequest(ctx, storageImpl, collection, wf, operationAttempt, re, storageTimer),
            storageTimer);
  }

  private @NotNull Result forwardWriteCollections(
          final @NotNull NakshaContext ctx,
          final @NotNull IStorage storageImpl,
          final @NotNull XyzCollection collection,
          final @NotNull WriteCollections<?, ?, ?> wc,
          final OperationAttempt operationAttempt,
          final StopWatch storageTimer) {
    logger.info("Processing WriteCollections against {}", collection.getId());
    if (isUpdateCollectionRequest(wc)) {
      if (properties.getAutoCreateCollection()) {
        return forwardWriteRequest(
                ctx,
                storageImpl,
                wc,
                re -> reattemptCollectionRequest(
                        ctx, storageImpl, collection, wc, operationAttempt, re, storageTimer),
                storageTimer);
      } else {
        logger.info(
                "Received update collection request but autoCreate is not enabled, returning success without any action");
        return new SuccessResult();
      }
    } else if (isPurgeCollectionRequest(wc)) {
      if (properties.getAutoDeleteCollection()) {
        return forwardWriteRequest(
                ctx,
                storageImpl,
                wc,
                re -> reattemptCollectionRequest(
                        ctx, storageImpl, collection, wc, operationAttempt, re, storageTimer),
                storageTimer);
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

  private boolean isUpdateCollectionRequest(@NotNull WriteCollections<?, ?, ?> wc) {
    final String op = wc.features.get(0).getOp();
    return wc.features.size() == 1
            && (EWriteOp.UPDATE.toString().equals(op)
            || EWriteOp.PUT.toString().equals(op));
  }

  private @NotNull Result forwardWriteRequest(
          @NotNull NakshaContext ctx,
          @NotNull IStorage storageImpl,
          @NotNull WriteRequest<?, ?, ?> wr,
          @NotNull F1<Result, RuntimeException> reattempt,
          @NotNull StopWatch storageTimer) {
    try {
      if (wr instanceof WriteXyzCollections){
        return measuredStorageSupplier(() -> performAtomicWriteCollection(ctx, storageImpl, (WriteXyzCollections) wr), storageTimer);
      } else{
        return measuredStorageSupplier(() -> performAtomicWriteFeatures(ctx, storageImpl, wr), storageTimer);
      }
    } catch (RuntimeException re) {
      return reattempt.call(re);
    }
  }

  private @NotNull Result performAtomicWriteCollection(
          @NotNull NakshaContext ctx,
          @NotNull IStorage storageImpl,
          @NotNull WriteXyzCollections writeCollections) {
    return singleWrite(ctx, storageImpl, writeCollections);
  }

  protected @NotNull Result performAtomicWriteFeatures(
          @NotNull NakshaContext ctx,
          @NotNull IStorage storageImpl,
          @NotNull WriteRequest<?, ?, ?> wr) {
    return singleWrite(ctx, storageImpl, wr);
  }

  private @NotNull Result singleWrite(
          @NotNull NakshaContext ctx, @NotNull IStorage storageImpl, @NotNull WriteRequest<?, ?, ?> wr) {
    try (final IWriteSession writer = storageImpl.newWriteSession(ctx, true)) {
      final Result result = writer.execute(wr);
      if (result instanceof SuccessResult) {
        writer.commit(true);
      } else {
        logger.warn("Failed executing {}, expected success but got: {}", wr.getClass(), result);
        writer.rollback(true);
      }
      return result;
    }
  }

  private @NotNull Result reattemptFeatureRequest(
          final @NotNull NakshaContext ctx,
          final @NotNull IStorage storageImpl,
          final @NotNull XyzCollection collection,
          final @NotNull Request<?> request,
          final @NotNull OperationAttempt previousAttempt,
          final @NotNull RuntimeException re,
          final @NotNull StopWatch storageTimer) {
    return switch (previousAttempt) {
      case FIRST_ATTEMPT -> reattemptFeatureRequestForTheFirstTime(
              ctx, storageImpl, collection, request, re, storageTimer);
      case ATTEMPT_AFTER_STORAGE_INITIALIZATION -> reattemptAfterStorageInitialization(
              ctx, storageImpl, collection, request, re, storageTimer);
      case ATTEMPT_AFTER_COLLECTION_CREATION -> throw re;
    };
  }

  private @NotNull Result reattemptCollectionRequest(
          NakshaContext ctx,
          IStorage storageImpl,
          XyzCollection collection,
          WriteCollections<?, ?, ?> wc,
          OperationAttempt previousAttempt,
          RuntimeException re,
          StopWatch storageTimer) {
    if (previousAttempt == FIRST_ATTEMPT && re instanceof StorageNotInitialized) {
      return retryDueToUninitializedStorage(ctx, storageImpl, collection, wc, storageTimer);
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
          final @NotNull RuntimeException re,
          final @NotNull StopWatch storageTimer) {
    if (re instanceof StorageNotInitialized) {
      return retryDueToUninitializedStorage(ctx, storageImpl, collection, request, storageTimer);
    } else if (indicatesMissingCollection(re)) {
      try {
        return retryDueToMissingCollection(ctx, storageImpl, collection, request, storageTimer);
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
          final @NotNull RuntimeException re,
          final @NotNull StopWatch storageTimer) {
    if (indicatesMissingCollection(re)) {
      try {
        return retryDueToMissingCollection(ctx, storageImpl, collection, request, storageTimer);
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
          final @NotNull Request<?> request,
          final @NotNull StopWatch storageTimer) {
    logger.info("Initializing Storage before reattempting write request.");
    measuredStorageRunnable(storageImpl::initStorage, storageTimer);
    logger.info("Storage initialized");
    return forwardRequestToStorage(
            ctx, request, storageImpl, collection, ATTEMPT_AFTER_STORAGE_INITIALIZATION, storageTimer);
  }

  private Result retryDueToMissingCollection(
          final @NotNull NakshaContext ctx,
          final @NotNull IStorage storageImpl,
          final @NotNull XyzCollection collection,
          final @NotNull Request<?> request,
          final @NotNull StopWatch storageTimer) {
    logger.warn("Collection not found for {}", collection.getId());
    if (properties.getAutoCreateCollection()) {
      logger.info(
              "Collection auto creation is enabled, attempting to create collection specified in request: {}",
              collection.getId());
      measuredStorageRunnable(() -> createXyzCollection(ctx, storageImpl, collection), storageTimer);
      logger.info("Created collection {}, forwarding the request once again", collection.getId());
      return forwardRequestToStorage(
              ctx, request, storageImpl, collection, ATTEMPT_AFTER_COLLECTION_CREATION, storageTimer);
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
  private @NotNull XyzCollection chooseCollection(final Request<?> request) {
    final XyzCollection collectionDefinedInHandler = properties.getXyzCollection();
    if (collectionDefinedInHandler != null) {
      logger.info(
              "Using collection with id {} that is associated with EventHandler(id={})",
              collectionDefinedInHandler.getId(),
              eventHandler.getId());
      return collectionDefinedInHandler;
    }
    if (eventTarget instanceof Space s) {
      XyzCollection collectionDefinedInSpace = null;
      if (request instanceof WriteCollections<?, ?, ?> wc && isUpdateCollectionRequest(wc)) {
        // use newly provided collection in the Update request itself
        // to make sure that the newer collection id (if it has been changed) is used
        collectionDefinedInSpace = (XyzCollection) wc.features.get(0).getFeature();
      } else {
        // use existing Space collection (as it is not an Update request)
        final SpaceProperties spaceProperties =
                JsonSerializable.convert(s.getProperties(), SpaceProperties.class);
        collectionDefinedInSpace = spaceProperties.getXyzCollection();
      }
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
      final Result result = writer.execute(createRequestForMissingCollections(collection));
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

  protected @NotNull WriteXyzCollections createRequestForMissingCollections(final @NotNull XyzCollection collection) {
    return RequestHelper.createWriteCollectionsRequest(collection);
  }

  enum OperationAttempt {
    FIRST_ATTEMPT,
    ATTEMPT_AFTER_STORAGE_INITIALIZATION,
    ATTEMPT_AFTER_COLLECTION_CREATION
  }
}
