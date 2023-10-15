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
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The read API of a transaction.
 */
@AvailableSince(NakshaVersion.v2_0_0)
public interface IReadTransaction extends AutoCloseable {

  @NotNull Result execute(@NotNull ReadRequest<?> readRequest);
  @NotNull Result process(@NotNull Notification<?> notification);

  /**
   * Returns the current transaction number, if none has been created yet, creating a new one.
   *
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_0)
  @NotNull
  String transactionNumber();

  /**
   * Returns the settings of the transaction.
   *
   * @return The settings of the transaction.
   */
  @Deprecated
  @NotNull
  ITransactionSettings settings();

  @AvailableSince(NakshaVersion.v2_0_0)
  @Override
  void close();

  /**
   * Iterate all collections from the storage.
   *
   * @return the iterator above all storage collections.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_0)
  @NotNull
  ClosableIterator<@NotNull CollectionInfo> iterateCollections();

  /**
   * Returns the collection with the given id.
   *
   * @param id the identifier of the collection to return.
   * @return the collection or {@code null}, if no such collection exists.
   */
  @Deprecated
  @AvailableSince(NakshaVersion.v2_0_0)
  @Nullable
  CollectionInfo getCollectionById(@NotNull String id);

  @Deprecated
  /**
   * Returns the reader for the given feature-type and collection.
   *
   * @param featureClass the class of the feature-type to read.
   * @param collection   the collection to read.
   * @param <FEATURE>    the feature-type.
   * @return the feature reader.
   */
  <FEATURE extends XyzFeature> @NotNull IFeatureReader<FEATURE> readFeatures(
      @NotNull Class<FEATURE> featureClass, @NotNull CollectionInfo collection);
}
