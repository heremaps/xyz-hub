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
package com.here.naksha.lib.core.models.storage;

import com.here.naksha.lib.core.NakshaVersion;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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

  @AvailableSince(NakshaVersion.v2_0_7)
  private boolean returnDeleted;

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

  @AvailableSince(NakshaVersion.v2_0_7)
  private @NotNull List<@NotNull String> collections;

  @AvailableSince(NakshaVersion.v2_0_7)
  private @Nullable Object spatialOp;

  public @Nullable POp getPropertyOp() {
    return propertyOp;
  }

  public void setPropertyOp(@Nullable POp propertyOp) {
    this.propertyOp = propertyOp;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  private @Nullable POp propertyOp;

  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull ReadFeatures withPropertyOp(@NotNull POp propertyOp) {
    this.propertyOp = propertyOp;
    return this;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  private @Nullable Object orderOp;
}
