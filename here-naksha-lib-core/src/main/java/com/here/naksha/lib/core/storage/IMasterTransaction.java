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
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** Interface to grant write-access to a storage. */
public interface IMasterTransaction extends IReadTransaction {

  @NotNull
  Result execute(@NotNull WriteRequest<?> writeRequest);

  @NotNull
  IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws TimeoutException;

  @NotNull
  IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit) throws TimeoutException;

  /**
   * Commit all changes.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  void commit();

  /** Abort the transaction, revert all pending changes. */
  @AvailableSince(NakshaVersion.v2_0_0)
  void rollback();

  /** Rollback everything that is still pending and close the writer. */
  @AvailableSince(NakshaVersion.v2_0_0)
  @Override
  void close();

  /** Acquire global (multi-instance) lock for a given key. */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_6)
  boolean acquireLock(final @NotNull String lockKey);

  /** Release global lock for a given key, which was previously acquired using acquireLock(). */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_6)
  boolean releaseLock(final @NotNull String lockKey);

  /**
   * Create a new collection, fails if the collection exists already.
   *
   * @param collection the collection to create.
   * @return the created collection.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_0)
  @NotNull
  CollectionInfo createCollection(@NotNull CollectionInfo collection);

  /**
   * Update the collection.
   *
   * @param collection The collection to update.
   * @return the updated collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_0)
  @NotNull
  CollectionInfo updateCollection(@NotNull CollectionInfo collection);

  /**
   * Update or insert the collection.
   *
   * @param collection The collection to update or insert.
   * @return the updated or inserted collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_0)
  @NotNull
  CollectionInfo upsertCollection(@NotNull CollectionInfo collection);

  /**
   * Deletes the collection, including the history, at the given point in time.
   *
   * @param collection the collection to delete.
   * @param deleteAt the unix epoch timestamp in milliseconds when to delete the table, must be
   *     greater than zero.
   * @return the dropped collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_0)
  @NotNull
  CollectionInfo deleteCollection(@NotNull CollectionInfo collection, long deleteAt);

  /**
   * Deletes the collection including the history instantly and not revertable.
   *
   * @param collection the collection to delete.
   * @return the dropped collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_4)
  @NotNull
  CollectionInfo dropCollection(@NotNull CollectionInfo collection);

  /**
   * Enable the history for the given collection.
   *
   * @param collection the collection on which to enable the history.
   * @return the modified collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_0)
  @NotNull
  CollectionInfo enableHistory(@NotNull CollectionInfo collection);

  /**
   * Disable the history for the given collection.
   *
   * @param collection The collection on which to disable the history.
   * @return the modified collection.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_0)
  @NotNull
  CollectionInfo disableHistory(@NotNull CollectionInfo collection);

  /**
   * Returns the writer for the given feature-type and collection.
   *
   * @param featureClass the class of the feature-type to read.
   * @param collection the collection to read.
   * @param <F> the feature-type.
   * @return the feature writer.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  // TODO HP_QUERY : Should be renamed to something like featureWriter() and similarly featureReader()
  @Deprecated
  <F extends XyzFeature> @NotNull IFeatureWriter<F> writeFeatures(
      @NotNull Class<F> featureClass, @NotNull CollectionInfo collection);
}
