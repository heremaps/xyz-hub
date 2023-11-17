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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A request to modify features into a collection of the storage.
 *
 * @param <FEATURE> The feature-type to write.
 * @param <CODEC>   The codec to use to encode features.
 * @param <SELF>    The self-type.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class WriteFeatures<
        FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>, SELF extends WriteFeatures<FEATURE, CODEC, SELF>>
    extends WriteRequest<FEATURE, CODEC, SELF> {

  /**
   * Creates a new empty feature write request.
   *
   * @param codecFactory The codec factory to use when creating new feature codecs.
   * @param collectionId The identifier of the collection to write into.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public WriteFeatures(@NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory, @NotNull String collectionId) {
    super(codecFactory);
    this.collectionId = collectionId;
  }

  /**
   * Returns the collection-id to write features into.
   *
   * @return the collection-id to write features into .
   */
  public @NotNull String getCollectionId() {
    return collectionId;
  }

  /**
   * Sets the collection-id to write features into.
   *
   * @param collectionId the collection-id to write features into.
   */
  public void setCollectionId(@NotNull String collectionId) {
    this.collectionId = collectionId;
  }

  /**
   * Sets the collection-id to write features into.
   *
   * @param collectionId the collection-id to write features into.
   * @return this.
   */
  public @NotNull SELF withCollectionId(@NotNull String collectionId) {
    this.collectionId = collectionId;
    return self();
  }

  /**
   * The identifier of the collection to write into.
   */
  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  private @NotNull String collectionId;
}
