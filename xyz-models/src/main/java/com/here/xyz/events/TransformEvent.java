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
import com.here.xyz.models.geojson.implementation.FeatureCollection;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "TransformEvent")
public class TransformEvent extends Event {

  private FeatureCollection featureCollection;
  private String crsId;
  private Integer level;
  private Integer x;
  private Integer y;

  @SuppressWarnings("unused")
  public FeatureCollection getFeatureCollection() {
    return this.featureCollection;
  }

  @SuppressWarnings("WeakerAccess")
  public void setFeatureCollection(FeatureCollection featureCollection) {
    this.featureCollection = featureCollection;
  }

  @SuppressWarnings("unused")
  public TransformEvent withFeatureCollection(FeatureCollection featureCollection) {
    setFeatureCollection(featureCollection);
    return this;
  }

  @SuppressWarnings("unused")
  public String getCrsId() {
    return this.crsId;
  }

  @SuppressWarnings("WeakerAccess")
  public void setCrsId(String crsId) {
    this.crsId = crsId;
  }

  @SuppressWarnings("unused")
  public TransformEvent withCrsId(String crsId) {
    setCrsId(crsId);
    return this;
  }

  @SuppressWarnings("unused")
  public Integer getLevel() {
    return this.level;
  }

  @SuppressWarnings("WeakerAccess")
  public void setLevel(int level) {
    this.level = level;
  }

  @SuppressWarnings("unused")
  public TransformEvent withLevel(int level) {
    setLevel(level);
    return this;
  }

  @SuppressWarnings("unused")
  public Integer getX() {
    return this.x;
  }

  @SuppressWarnings("WeakerAccess")
  public void setX(int x) {
    this.x = x;
  }

  @SuppressWarnings("unused")
  public TransformEvent withX(int x) {
    setX(x);
    return this;
  }

  @SuppressWarnings("unused")
  public Integer getY() {
    return this.y;
  }

  @SuppressWarnings("WeakerAccess")
  public void setY(int y) {
    this.y = y;
  }

  @SuppressWarnings("unused")
  public TransformEvent withY(int y) {
    setY(y);
    return this;
  }
}
