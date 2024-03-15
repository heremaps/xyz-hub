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
package com.here.naksha.lib.core.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.coordinates.MultiLineStringCoordinates;
import com.here.naksha.lib.core.models.geojson.exceptions.InvalidGeometryException;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "MultiLineString")
public class XyzMultiLineString extends XyzGeometryItem {

  @JsonProperty(COORDINATES)
  private MultiLineStringCoordinates coordinates = new MultiLineStringCoordinates();

  @Override
  @JsonGetter
  public MultiLineStringCoordinates getCoordinates() {
    return this.coordinates;
  }

  @JsonSetter
  public void setCoordinates(MultiLineStringCoordinates coordinates) {
    this.coordinates = coordinates;
  }

  public XyzMultiLineString withCoordinates(MultiLineStringCoordinates coordinates) {
    setCoordinates(coordinates);
    return this;
  }

  @Override
  protected org.locationtech.jts.geom.MultiLineString convertToJTSGeometry() {
    return JTSHelper.toMultiLineString(getCoordinates());
  }

  @Override
  public void validate() throws InvalidGeometryException {
    validateMultiLineStringCoordinates(this.coordinates);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    XyzMultiLineString that = (XyzMultiLineString) o;
    return Objects.equals(coordinates, that.coordinates);
  }

  @Override
  public int hashCode() {
    return Objects.hash(coordinates);
  }
}
