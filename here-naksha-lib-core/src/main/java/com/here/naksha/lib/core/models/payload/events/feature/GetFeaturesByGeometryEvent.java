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
package com.here.naksha.lib.core.models.payload.events.feature;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.implementation.Geometry;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "GetFeaturesByGeometryEvent")
public class GetFeaturesByGeometryEvent extends SpatialQueryEvent {

  private int radius;
  private Geometry geometry;
  private String h3Index;

  @SuppressWarnings("unused")
  public String getH3Index() {
    return h3Index;
  }

  public void setH3Index(String h3Index) {
    this.h3Index = h3Index;
  }

  @SuppressWarnings("unused")
  public GetFeaturesByGeometryEvent withH3Index(String h3Index) {
    setH3Index(h3Index);
    return this;
  }

  @SuppressWarnings("unused")
  public int getRadius() {
    return radius;
  }

  public void setRadius(int radius) {
    this.radius = radius;
  }

  @SuppressWarnings("unused")
  public GetFeaturesByGeometryEvent withRadius(int radius) {
    setRadius(radius);
    return this;
  }

  public Geometry getGeometry() {
    return geometry;
  }

  public void setGeometry(Geometry geometry) {
    this.geometry = geometry;
  }

  public GetFeaturesByGeometryEvent withGeometry(Geometry geometry) {
    setGeometry(geometry);
    return this;
  }
}
