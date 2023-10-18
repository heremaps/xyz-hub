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
package com.here.naksha.lib.core.storage;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A storage session that can perform changes.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public interface IWriteSession extends IReadSession {

  /**
   * Execute the given write-request.
   *
   * @param writeRequest the write-request to execute.
   * @return the result.
   */
  @SuppressWarnings("rawtypes")
  @AvailableSince(NakshaVersion.v2_0_7)
  @NotNull
  Result execute(@NotNull WriteRequest writeRequest);

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
  @AvailableSince(NakshaVersion.v2_0_7)
  @NotNull
  IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException;

  /**
   * Acquire an advisory lock.
   *
   * @param lockId   the unique identifier of the lock to acquire.
   * @param timeout  the maximum time to wait for the lock.
   * @param timeUnit the time-unit in which the wait-time was provided.
   * @return the lock.
   * @throws StorageLockException if the locking failed.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @NotNull
  IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException;

  /**
   * Commit all changes.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  void commit();

  /**
   * Abort the transaction, revert all pending changes.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  void rollback();
}
