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
package com.here.naksha.lib.core.models.storage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_7)
public class ReadFeatures extends ReadRequest<ReadFeatures> {

  @AvailableSince(NakshaVersion.v2_0_7)
  public ReadFeatures() {
    collections = new ArrayList<>();
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public ReadFeatures(@NotNull String... collectionIds) {
    collections = new ArrayList<>(collectionIds.length);
    //noinspection ManualArrayToCollectionCopy
    for (final String collectionId : collectionIds) {
      //noinspection UseBulkOperation
      collections.add(collectionId);
    }
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public ReadFeatures(@Nullable List<@NotNull String> collections) {
    this.collections = collections != null ? collections : new ArrayList<>();
  }

  /**
   * Weather to return deleted features, normally {@code false}.
   */
  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  protected boolean returnDeleted;

  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull ReadFeatures withReturnDeleted(boolean returnDeleted) {
    this.returnDeleted = returnDeleted;
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  private boolean returnAllVersions;

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull List<@NotNull String> getCollections() {
    return collections;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public void setCollections(@NotNull List<@NotNull String> collections) {
    this.collections = collections;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull ReadFeatures withCollections(@NotNull List<@NotNull String> collections) {
    this.collections = collections;
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull ReadFeatures addCollection(@NotNull String collectionId) {
    collections.add(collectionId);
    return this;
  }

  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  private @NotNull List<@NotNull String> collections;

  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  private @Nullable SOp spatialOp;

  /**
   * Returns the operation that should be applied to filter features by their geometry.
   *
   * @return the operation that should be applied to filter features by their geometry.
   */
  public @Nullable SOp getSpatialOp() {
    return spatialOp;
  }

  /**
   * Sets the operation that should be applied to filter features by their geometry.
   *
   * @param spatialOp The operation that should be applied to filter features by their geometry.
   * @return The previously assigned operation.
   */
  public @Nullable SOp setSpatialOp(@Nullable SOp spatialOp) {
    final SOp old = this.spatialOp;
    this.spatialOp = spatialOp;
    return old;
  }

  /**
   * Sets the operation that should be applied to filter features by their geometry.
   *
   * @param spatialOp The operation that should be applied to filter features by their geometry.
   * @return The previously assigned operation.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull ReadFeatures withSpatialOp(@Nullable SOp spatialOp) {
    this.spatialOp = spatialOp;
    return this;
  }

  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  private @Nullable POp propertyOp;

  /**
   * Returns the operation that should be applied to filter features by their property state.
   *
   * @return the operation that should be applied to filter features by their property state.
   */
  public @Nullable POp getPropertyOp() {
    return propertyOp;
  }

  /**
   * Sets the operation that should be applied to filter features by their property state.
   *
   * @param propertyOp The operation that should be applied to filter features by their property state.
   * @return The previously assigned operation.
   */
  public @Nullable POp setPropertyOp(@Nullable POp propertyOp) {
    final POp old = this.propertyOp;
    this.propertyOp = propertyOp;
    return old;
  }

  /**
   * Sets the operation that should be applied to filter features by their property state.
   *
   * @param propertyOp The operation that should be applied to filter features by their property state.
   * @return The previously assigned operation.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull ReadFeatures withPropertyOp(@Nullable POp propertyOp) {
    this.propertyOp = propertyOp;
    return this;
  }
}
