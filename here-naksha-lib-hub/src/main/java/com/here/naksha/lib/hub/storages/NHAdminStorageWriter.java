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
package com.here.naksha.lib.hub.storages;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.storage.IWriteSession;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;

public class NHAdminStorageWriter extends NHAdminStorageReader implements IWriteSession {

  /** Current session, all write storage operations should be executed against */
  final @NotNull IWriteSession session;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public NHAdminStorageWriter(final @NotNull IWriteSession writer) {
    super(writer);
    this.session = writer;
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
    return session.execute(writeRequest);
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
    return session.lockFeature(collectionId, featureId, timeout, timeUnit);
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
    return session.lockStorage(lockId, timeout, timeUnit);
  }

  /**
   * Commit all changes.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void commit(boolean autoCloseCursors) {
    session.commit(autoCloseCursors);
  }

  /**
   * Abort the transaction, revert all pending changes.
   */
  @Override
  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  public void rollback(boolean autoCloseCursors) {
    session.rollback(autoCloseCursors);
  }

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
  public void close(boolean autoCloseCursors) {
    session.close(autoCloseCursors);
  }
}
