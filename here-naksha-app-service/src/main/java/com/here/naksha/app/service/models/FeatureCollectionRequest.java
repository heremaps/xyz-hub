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
package com.here.naksha.app.service.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.LazyParsableFeatureList;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.util.List;
import org.jetbrains.annotations.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "FeatureCollection")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class FeatureCollectionRequest {

  private final @NotNull LazyParsableFeatureList features;

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  private String nextPageToken;

  public FeatureCollectionRequest() {
    features = new LazyParsableFeatureList();
  }

  public @NotNull List<? extends XyzFeature> getFeatures() {
    return features.get();
  }

  public void setFeatures(@NotNull List<? extends XyzFeature> features) {
    this.features.set(features);
  }

  public @NotNull FeatureCollectionRequest withFeatures(final @NotNull List<? extends @NotNull XyzFeature> features) {
    setFeatures(features);
    return this;
  }

  public @NotNull LazyParsableFeatureList getLazyParsableFeatureList() {
    return features;
  }

  @SuppressWarnings({"unchecked"})
  public void setLazyParsableFeatureList(Object features) {
    if (features instanceof String string) {
      this.features.set(string);
    } else if (features instanceof List<?> list) {
      this.features.set((List<XyzFeature>) list);
    }
  }

  /**
   * Returns the Space nextPageToken which is used to iterate above data.
   *
   * @return the nextPageToken.
   */
  public String getNextPageToken() {
    return this.nextPageToken;
  }

  /**
   * Sets the Space nextPageToken that can be used to continue an iterate.
   *
   * @param nextPageToken the nextPageToken, if null the nextPageToken property is removed.
   */
  public void setNextPageToken(String nextPageToken) {
    this.nextPageToken = nextPageToken;
  }

  public @NotNull FeatureCollectionRequest withNextPageToken(final String nextPageToken) {
    setNextPageToken(nextPageToken);
    return this;
  }
}
