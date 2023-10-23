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
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteCollections;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class NHSpaceStorageWriter extends NHSpaceStorageReader implements IWriteSession {

  public NHSpaceStorageWriter(
      final @NotNull INaksha hub,
      final @NotNull Map<String, List<IEventHandler>> virtualSpaces,
      final @Nullable NakshaContext context,
      boolean useMaster) {
    super(hub, virtualSpaces, context, useMaster);
  }

  /**
   * Execute the given write-request.
   *
   * @param writeRequest the write-request to execute.
   * @return the result.
   */
  @Override
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
    if (virtualSpaces.containsKey(wf.collectionId)) {
      // Request is to write to Naksha Admin space
      return executeWriteFeaturesToAdminSpaces(wf);
    } else {
      // Request is to write to Custom space
      return executeWriteFeaturesToCustomSpaces(wf);
    }
  }

  private @NotNull Result executeWriteFeaturesToAdminSpaces(final @NotNull WriteFeatures wf) {
    // Run pipeline against virtual space
    final EventPipeline pipeline = new EventPipeline(nakshaHub);
    // add internal Admin resource specific event handlers
    for (final IEventHandler handler : virtualSpaces.get(wf.collectionId)) {
      pipeline.addEventHandler(handler);
    }
    return pipeline.sendEvent(wf);
  }

  private @NotNull Result executeWriteFeaturesToCustomSpaces(final @NotNull WriteFeatures rf) {
    // TODO : Add logic to support running pipeline for custom space
    throw new UnsupportedOperationException("WriteFeatures to custom space not supported as of now");
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
  public @NotNull IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    throw new UnsupportedOperationException("Locking not supported by this storage instance!");
  }

  /**
   * Commit all changes.
   */
  @Override
  public void commit() {}

  /**
   * Abort the transaction, revert all pending changes.
   */
  @Override
  public void rollback() {}
}
