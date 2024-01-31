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
package com.here.naksha.lib.handlers;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.naksha.SpaceProperties;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Default variant of EventHandler properties supported by Naksha - default storage handler
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class DefaultStorageHandlerProperties extends XyzProperties {

  private static final Boolean DEFAULT_AUTO_CREATE_COLLECTION = true;
  private static final Boolean DEFAULT_AUTO_DELETE_COLLECTION = true;

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String STORAGE_ID = "storageId";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String COLLECTION = "collection";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String AUTO_CREATE_COLLECTION = "autoCreateCollection";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String AUTO_DELETE_COLLECTION = "autoDeleteCollection";

  /**
   * To associate EventHandler with specific {@link Storage} that it should operate against.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(STORAGE_ID)
  private @Nullable String storageId;

  /**
   * Details of the backend xyz collection to use.
   * If undefined, the collection defined at the {@link SpaceProperties} level will be used.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(COLLECTION)
  private @Nullable XyzCollection xyzCollection;

  /**
   * Indicates whether collection should be created automatically (happens on first collection's usage).
   * By default: 'true'
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(AUTO_CREATE_COLLECTION)
  private @NotNull Boolean autoCreateCollection;

  /**
   * Indicates whether collection should be deleted automatically (happens when handler is deleted).
   * By default: 'true'
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(AUTO_DELETE_COLLECTION)
  private @NotNull Boolean autoDeleteCollection;

  /**
   * Create new EventHandler properties with storageId and collection details
   *
   * @param xyzCollection details of backend xyz collection
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonCreator
  public DefaultStorageHandlerProperties(
      final @JsonProperty(STORAGE_ID) @Nullable String storageId,
      final @JsonProperty(COLLECTION) @Nullable XyzCollection xyzCollection,
      final @JsonProperty(AUTO_CREATE_COLLECTION) Boolean autoCreateCollection,
      final @JsonProperty(AUTO_DELETE_COLLECTION) Boolean autoDeleteCollection) {
    this.storageId = storageId;
    this.xyzCollection = xyzCollection;
    this.autoCreateCollection =
        autoCreateCollection == null ? DEFAULT_AUTO_CREATE_COLLECTION : autoCreateCollection;
    this.autoDeleteCollection =
        autoDeleteCollection == null ? DEFAULT_AUTO_DELETE_COLLECTION : autoDeleteCollection;
  }

  public @Nullable XyzCollection getXyzCollection() {
    return xyzCollection;
  }

  public void setXyzCollection(final @JsonProperty(COLLECTION) @Nullable XyzCollection xyzCollection) {
    this.xyzCollection = xyzCollection;
  }

  public @Nullable String getStorageId() {
    return storageId;
  }

  public void setStorageId(final @Nullable String storageId) {
    this.storageId = storageId;
  }

  public @NotNull Boolean getAutoCreateCollection() {
    return autoCreateCollection;
  }

  public void setAutoCreateCollection(Boolean autoCreateCollection) {
    this.autoCreateCollection = autoCreateCollection;
  }

  public @NotNull Boolean getAutoDeleteCollection() {
    return autoDeleteCollection;
  }

  public void setAutoDeleteCollection(Boolean autoDeleteCollection) {
    this.autoDeleteCollection = autoDeleteCollection;
  }
}
