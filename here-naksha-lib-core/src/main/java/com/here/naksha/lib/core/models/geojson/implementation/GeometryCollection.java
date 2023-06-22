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

package com.here.naksha.lib.core.models.geojson.implementation;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.exceptions.InvalidGeometryException;
import java.util.ArrayList;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "GeometryCollection")
public class GeometryCollection extends Geometry {

  private List<GeometryItem> geometries = new ArrayList<>();

  public List<GeometryItem> getGeometries() {
    return this.geometries;
  }

  @SuppressWarnings("WeakerAccess")
  public void setGeometries(List<GeometryItem> geometries) {
    this.geometries = geometries;
  }

  public GeometryCollection withGeometries(final List<GeometryItem> geometries) {
    setGeometries(geometries);
    return this;
  }

  @Override
  public BBox calculateBBox() {
    if (this.geometries == null || this.geometries.size() == 0) {
      return null;
    }

    double minLon = Double.MAX_VALUE;
    double minLat = Double.MAX_VALUE;
    double maxLon = Double.MIN_VALUE;
    double maxLat = Double.MIN_VALUE;

    for (GeometryItem geom : this.geometries) {
      BBox bbox = geom.calculateBBox();
      if (bbox != null) {
        if (bbox.minLon() < minLon) {
          minLon = bbox.minLon();
        }
        if (bbox.minLat() < minLat) {
          minLat = bbox.minLat();
        }
        if (bbox.maxLon() > maxLon) {
          maxLon = bbox.maxLon();
        }
        if (bbox.maxLat() > maxLat) {
          maxLat = bbox.maxLat();
        }
      }
    }

    if (minLon != Double.MAX_VALUE
        && minLat != Double.MAX_VALUE
        && maxLon != Double.MIN_VALUE
        && maxLat != Double.MIN_VALUE) {
      return new BBox(minLon, minLat, maxLon, maxLat);
    }
    return null;
  }

  @Override
  protected com.vividsolutions.jts.geom.GeometryCollection convertToJTSGeometry() {
    if (this.geometries == null || this.geometries.size() == 0) {
      return null;
    }

    com.vividsolutions.jts.geom.Geometry[] jtsGeometries =
        new com.vividsolutions.jts.geom.Geometry[this.geometries.size()];
    for (int i = 0; i < jtsGeometries.length; i++) {
      jtsGeometries[i] = this.geometries.get(i).getJTSGeometry();
    }

    return JTSHelper.factory.createGeometryCollection(jtsGeometries);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void validate() throws InvalidGeometryException {
    if (this.geometries == null || this.geometries.size() == 0) {
      return;
    }

    for (GeometryItem geometry : this.geometries) {
      try {
        geometry.validate();
      } catch (InvalidGeometryException e) {
        throw new InvalidGeometryException("The geometry with type "
            + geometry.getClass().getSimpleName()
            + " is invalid, reason: "
            + e.getMessage());
      }
    }
  }
}
