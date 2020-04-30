/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.models.geojson.coordinates.BBox;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SpatialQueryEvent<T extends SpatialQueryEvent> extends SearchForFeaturesEvent<T> {

  private long simplificationLevel;
  private String clusteringType;
  private Map<String, Object> clusteringParams;
  private String tweakType;
  private Map<String, Object> tweakParams;

  @SuppressWarnings("unused")
  public long getSimplificationLevel() {
    return this.simplificationLevel;
  }

  @SuppressWarnings("WeakerAccess")
  public void setSimplificationLevel(long simplificationLevel) {
    this.simplificationLevel = simplificationLevel;
  }

  @SuppressWarnings("unused")
  public T withSimplificationLevel(long simplificationLevel) {
    setSimplificationLevel(simplificationLevel);
    //noinspection unchecked
    return (T) this;
  }

  @SuppressWarnings("unused")
  public String getClusteringType() {
    return this.clusteringType;
  }

  @SuppressWarnings("WeakerAccess")
  public void setClusteringType(String clusteringType) {
    this.clusteringType = clusteringType;
  }

  @SuppressWarnings("unused")
  public T withClusteringType(String clusteringType) {
    setClusteringType(clusteringType);
    //noinspection unchecked
    return (T) this;
  }

  @SuppressWarnings("unused")
  public Map<String, Object> getClusteringParams() {
    return this.clusteringParams;
  }

  public void setClusteringParams(Map<String, Object> clusteringParams) {
    this.clusteringParams = clusteringParams;
  }

  @SuppressWarnings({"unused", "unchecked"})
  public T withClusteringParams(Map<String, Object> clusteringParams) {
    this.clusteringParams = clusteringParams;
    return (T) this;
  }

  @SuppressWarnings("unused")
  public Map<String, Object> getTweakParams() {
    return this.tweakParams;
  }

  public void setTweakParams(Map<String, Object> tweakParams) {
    this.tweakParams = tweakParams;
  }

  @SuppressWarnings({"unused", "unchecked"})
  public T withTweakParams(Map<String, Object> tweakParams) {
    this.tweakParams = tweakParams;
    return (T) this;
  }

  @SuppressWarnings("unused")
  public String getTweakType() {
    return this.tweakType;
  }

  @SuppressWarnings("WeakerAccess")
  public void setTweakType(String tweakType) {
    this.tweakType = tweakType;
  }

  @SuppressWarnings("unused")
  public T withTweakType(String tweakType) {
    setTweakType(tweakType);
    //noinspection unchecked
    return (T) this;
  }
}
