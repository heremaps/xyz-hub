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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.coordinates.LineStringCoordinates;
import com.here.naksha.lib.core.models.geojson.exceptions.InvalidGeometryException;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "LineString")
public class XyzLineString extends XyzGeometryItem {

  @JsonProperty(COORDINATES)
  LineStringCoordinates coordinates = new LineStringCoordinates();

  @Override
  @JsonGetter
  public LineStringCoordinates getCoordinates() {
    return this.coordinates;
  }

  @JsonSetter
  public void setCoordinates(LineStringCoordinates coordinates) {
    this.coordinates = coordinates;
  }

  public XyzLineString withCoordinates(LineStringCoordinates coordinates) {
    setCoordinates(coordinates);
    return this;
  }

  public org.locationtech.jts.geom.LineString convertToJTSGeometry() {
    return JTSHelper.toLineString(this.coordinates);
  }

  @Override
  public void validate() throws InvalidGeometryException {
    validateLineStringCoordinates(this.coordinates);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    XyzLineString that = (XyzLineString) o;
    return Objects.equals(coordinates, that.coordinates);
  }

  @Override
  public int hashCode() {
    return Objects.hash(coordinates);
  }
}
