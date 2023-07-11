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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.naksha.lib.core.models.geojson.declaration.IBoundedCoordinates;

@JsonSubTypes({
  @JsonSubTypes.Type(value = XyzPoint.class, name = "Point"),
  @JsonSubTypes.Type(value = XyzMultiPoint.class, name = "MultiPoint"),
  @JsonSubTypes.Type(value = XyzLineString.class, name = "LineString"),
  @JsonSubTypes.Type(value = XyzMultiLineString.class, name = "MultiLineString"),
  @JsonSubTypes.Type(value = XyzPolygon.class, name = "Polygon"),
  @JsonSubTypes.Type(value = XyzMultiPolygon.class, name = "MultiPolygon")
})
public abstract class XyzGeometryItem extends XyzGeometry {

  public abstract IBoundedCoordinates getCoordinates();

  @Override
  public BBox calculateBBox() {
    final IBoundedCoordinates coordinates = getCoordinates();
    if (coordinates != null) {
      return coordinates.calculateBBox();
    }

    return null;
  }
}
