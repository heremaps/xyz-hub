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
package com.here.naksha.lib.core.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.coordinates.PointCoordinates;
import com.here.naksha.lib.core.models.geojson.exceptions.InvalidGeometryException;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Point")
public class XyzPoint extends XyzGeometryItem {

  public XyzPoint() {}

  public XyzPoint(double longitude, double latitude) {
    this(longitude, latitude, 0d);
  }

  public XyzPoint(double longitude, double latitude, double altitude) {
    coordinates.add(longitude);
    coordinates.add(latitude);
    coordinates.add(altitude);
  }

  @JsonProperty(COORDINATES)
  private PointCoordinates coordinates = new PointCoordinates();

  @Override
  @JsonGetter
  public PointCoordinates getCoordinates() {
    return this.coordinates;
  }

  @JsonSetter
  public void setCoordinates(PointCoordinates coordinates) {
    this.coordinates = coordinates;
  }

  public @NotNull XyzPoint withCoordinates(PointCoordinates coordinates) {
    setCoordinates(coordinates);
    return this;
  }

  @JsonIgnore
  public org.locationtech.jts.geom.Point convertToJTSGeometry() {
    return JTSHelper.toPoint(this.coordinates);
  }

  @Override
  public void validate() throws InvalidGeometryException {
    validatePointCoordinates(this.coordinates);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    XyzPoint xyzPoint = (XyzPoint) o;
    return Objects.equals(coordinates, xyzPoint.coordinates);
  }

  @Override
  public int hashCode() {
    return Objects.hash(coordinates);
  }
}
