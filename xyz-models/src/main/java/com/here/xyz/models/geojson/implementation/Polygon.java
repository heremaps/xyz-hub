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

package com.here.xyz.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.models.geojson.coordinates.JTSHelper;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Polygon")
public class Polygon extends GeometryItem {

  private PolygonCoordinates coordinates = new PolygonCoordinates();

  @Override
  public PolygonCoordinates getCoordinates() {
    return this.coordinates;
  }

  public void setCoordinates(PolygonCoordinates coordinates) {
    this.coordinates = coordinates;
  }

  public Polygon withCoordinates(PolygonCoordinates coordinates) {
    setCoordinates(coordinates);
    return this;
  }

  @Override
  protected org.locationtech.jts.geom.Geometry convertToJTSGeometry() {
    return JTSHelper.toPolygon(this.coordinates);
  }

  @Override
  public void validate() throws InvalidGeometryException {
    validatePolygonCoordinates(this.coordinates);
  }
}
