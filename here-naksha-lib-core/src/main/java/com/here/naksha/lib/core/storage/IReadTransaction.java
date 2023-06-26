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

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import java.util.Iterator;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The read API of a transaction. */
@AvailableSince(INaksha.v2_0_0)
public interface IReadTransaction extends AutoCloseable {
  /**
   * Returns the current transaction number, if none has been created yet, creating a new one.
   *
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @NotNull
  String getTransactionNumber() throws Exception;

  @AvailableSince(INaksha.v2_0_0)
  @Override
  void close();

  /**
   * Iterate all collections from the storage.
   *
   * @return the iterator above all storage collections.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @NotNull
  Iterator<@NotNull CollectionInfo> iterateCollections() throws Exception;

  /**
   * Returns the collection with the given id.
   *
   * @param id the identifier of the collection to return.
   * @return the collection or {@code null}, if no such collection exists.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  @AvailableSince(INaksha.v2_0_0)
  @Nullable
  CollectionInfo getCollectionById(@NotNull String id) throws Exception;

  /**
   * Returns the reader for the given feature-type and collection.
   *
   * @param featureClass the class of the feature-type to read.
   * @param collection the collection to read.
   * @param <FEATURE> the feature-type.
   * @return the feature reader.
   * @throws Exception if access to the storage failed or any other error occurred.
   */
  <FEATURE extends Feature> @NotNull IFeatureReader<FEATURE> readFeatures(
      @NotNull Class<FEATURE> featureClass, @NotNull CollectionInfo collection) throws Exception;
}
