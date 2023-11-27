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

import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.models.storage.WriteCollections;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.hub.EventPipelineFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NHSpaceStorageWriter extends NHSpaceStorageReader implements IWriteSession {

  private static final Logger logger = LoggerFactory.getLogger(NHSpaceStorageWriter.class);

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public NHSpaceStorageWriter(
      final @NotNull INaksha hub,
      final @NotNull Map<String, List<IEventHandler>> virtualSpaces,
      final @NotNull EventPipelineFactory pipelineFactory,
      final @Nullable NakshaContext context,
      boolean useMaster) {
    super(hub, virtualSpaces, pipelineFactory, context, useMaster);
  }

  /**
   * Execute the given write-request.
   *
   * @param writeRequest the write-request to execute.
   * @return the result.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull Result execute(@NotNull WriteRequest writeRequest) {
    if (writeRequest instanceof WriteCollections wc) {
      return executeWriteCollections(wc);
    } else if (writeRequest instanceof WriteFeatures wf) {
      return executeWriteFeatures(wf);
    }
    throw new UnsupportedOperationException(
        "WriteRequest with unsupported type " + writeRequest.getClass().getName());
  }

  private @NotNull Result executeWriteCollections(final @NotNull WriteCollections wc) {
    try (final IWriteSession admin = nakshaHub.getAdminStorage().newWriteSession(context, useMaster)) {
      return admin.execute(wc);
    }
  }

  private @NotNull Result executeWriteFeatures(final @NotNull WriteFeatures wf) {
    final String spaceId = wf.getCollectionId();
    addSpaceIdToStreamInfo(spaceId);
    if (virtualSpaces.containsKey(spaceId)) {
      // Request is to write to Naksha Admin space
      return executeWriteFeaturesToAdminSpaces(wf);
    } else {
      // Request is to write to Custom space
      return executeWriteFeaturesToCustomSpaces(wf);
    }
  }

  private @NotNull Result executeWriteFeaturesToAdminSpaces(final @NotNull WriteFeatures<?, ?, ?> wf) {
    // Run pipeline against virtual space
    final EventPipeline pipeline = pipelineFactory.eventPipeline();
    // add internal Admin resource specific event handlers
    for (final IEventHandler handler : virtualSpaces.get(wf.getCollectionId())) {
      pipeline.addEventHandler(handler);
    }
    return pipeline.sendEvent(wf);
  }

  private @NotNull Result executeWriteFeaturesToCustomSpaces(final @NotNull WriteFeatures<?, ?, ?> wf) {
    final String spaceId = wf.getCollectionId();
    final EventPipeline eventPipeline = pipelineFactory.eventPipeline();
    final Result result = setupEventPipelineForSpaceId(spaceId, eventPipeline);
    if (!(result instanceof SuccessResult)) return result;
    return eventPipeline.sendEvent(wf);
  }

  /**
   * Acquire a lock to a specific feature in the HEAD state.
   *
   * @param collectionId the collection in which the feature is stored.
   * @param featureId    the identifier of the feature to lock.
   * @param timeout      the maximum time to wait for the lock.
   * @param timeUnit     the time-unit in which the wait-time was provided.
   * @return the lock.
   * @throws StorageLockException if the locking failed.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    throw new UnsupportedOperationException("Locking not supported by this storage instance!");
  }

  /**
   * Acquire an advisory lock.
   *
   * @param lockId   the unique identifier of the lock to acquire.
   * @param timeout  the maximum time to wait for the lock.
   * @param timeUnit the time-unit in which the wait-time was provided.
   * @return the lock.
   * @throws StorageLockException if the locking failed.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    throw new UnsupportedOperationException("Locking not supported by this storage instance!");
  }

  /**
   * Commit all changes.
   * <p>
   * Beware setting {@code autoCloseCursors} to {@code true} is often very suboptimal. To keep cursors alive, most of the time the
   * implementation requires to read all results synchronously from all open cursors in an in-memory cache and to close the underlying
   * network resources. This can lead to {@link OutOfMemoryError}'s or other issues. It is strictly recommended to first read from all open
   * cursors before closing, committing or rolling-back a session.
   *
   * @param autoCloseCursors If {@code true}, all open cursors are closed; otherwise all pending cursors are kept alive.
   */
  @Override
  public void commit(boolean autoCloseCursors) {}

  /**
   * Abort the transaction, revert all pending changes.
   * <p>
   * Beware setting {@code autoCloseCursors} to {@code true} is often very suboptimal. To keep cursors alive, most of the time the
   * implementation requires to read all results synchronously from all open cursors in an in-memory cache and to close the underlying
   * network resources. This can lead to {@link OutOfMemoryError}'s or other issues. It is strictly recommended to first read from all open
   * cursors before closing, committing or rolling-back a session.
   *
   * @param autoCloseCursors If {@code true}, all open cursors are closed; otherwise all pending cursors are kept alive.
   */
  @Override
  public void rollback(boolean autoCloseCursors) {}

  /**
   * Closes the session and, when necessary invokes {@link #rollback(boolean)}.
   * <p>
   * Beware setting {@code autoCloseCursors} to {@code true} is often very suboptimal. To keep cursors alive, most of the time the
   * implementation requires to read all results synchronously from all open cursors in an in-memory cache and to close the underlying
   * network resources. This can lead to {@link OutOfMemoryError}'s or other issues. It is strictly recommended to first read from all open
   * cursors before closing, committing or rolling-back a session.
   *
   * @param autoCloseCursors If {@code true}, all open cursors are closed; otherwise all pending cursors are kept alive.
   */
  @Override
  public void close(boolean autoCloseCursors) {}
}
