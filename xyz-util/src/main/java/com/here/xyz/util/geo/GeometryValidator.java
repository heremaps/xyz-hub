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

package com.here.xyz.util.geo;

import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;

public class GeometryValidator {
  public static int MAX_NUMBER_OF_COORDNIATES = 12_000;

  public static void validateGeometry(Geometry geometry, int radius) throws GeometryException {
    if (geometry == null)
      throw new GeometryException("Invalid arguments! Geometry cant be null!");

    try {
      geometry.validate();
      int nrCoordinates = geometry.getJTSGeometry().getNumPoints();

      if(nrCoordinates > MAX_NUMBER_OF_COORDNIATES)
        throw new GeometryException(String.format("Invalid arguments! Geometry exceeds %d coordinates < %d coordinates", MAX_NUMBER_OF_COORDNIATES, nrCoordinates));

      if(radius > 0 && ! (geometry instanceof Point)
              && GeoTools.geometryCrossesDateline(geometry, radius)) {
        throw new GeometryException("Invalid arguments! geometry filter intersects with antimeridian!");
      }
    }
    catch (Exception e){
      throw new GeometryException("Invalid filter geometry!");
    }
  }

  public static boolean isWorldBoundingBox(Geometry geometry) {
    if (!(geometry instanceof Polygon)) {
      return false;
    }

    Polygon polygon = (Polygon) geometry;
    PolygonCoordinates coordinates = polygon.getCoordinates();

    PolygonCoordinates expectedCoordinates = new PolygonCoordinates();
    // Expected coordinates of a world bounding box
    LinearRingCoordinates lrc = new LinearRingCoordinates();
    lrc.add(new Position(-180.0, -90.0));
    lrc.add(new Position(180.0, -90.0));
    lrc.add(new Position(180.0, 90.0));
    lrc.add(new Position(-180.0, 90.0));
    lrc.add(new Position(-180.0, -90.0)); // close the ring
    expectedCoordinates.add(lrc);

    PolygonCoordinates inputCords = ((Polygon) geometry).getCoordinates();
    if(inputCords.size() == 0)
      return false;

    LinearRingCoordinates inputLrc = inputCords.get(0);

    if (lrc.size() != inputLrc.size()) {
      return false;
    }

    for (int i = 0; i < lrc.size(); i++) {
      if (!lrc.get(i).isEqual(inputLrc.get(i)) ) {
        return false;
      }
    }

    return true;
  }

  public static class GeometryException extends Exception {
    public GeometryException(String message) {
      super(message);
    }
  }
}
