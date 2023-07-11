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
package com.here.naksha.lib.core.models.geojson.coordinates;

import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.models.geojson.implementation.XyzLineString;
import com.here.naksha.lib.core.models.geojson.implementation.XyzMultiLineString;
import com.here.naksha.lib.core.models.geojson.implementation.XyzMultiPoint;
import com.here.naksha.lib.core.models.geojson.implementation.XyzMultiPolygon;
import com.here.naksha.lib.core.models.geojson.implementation.XyzPoint;
import com.here.naksha.lib.core.models.geojson.implementation.XyzPolygon;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WKTHelper {

  private static String coordinatesToWKB(List<Position> postionList) {
    String ret = "(";
    for (Position position : postionList) {
      ret += position.getLongitude()
          + " "
          + position.getLatitude()
          + " "
          + (position.getAltitude() == null ? "" : position.getAltitude())
          + ",";
    }
    return ret.substring(0, ret.length() - 1) + ")";
  }

  private static String linearRingsToWKB(List<LinearRingCoordinates> coordinates) {
    String ret = "(";
    for (LinearRingCoordinates position : coordinates) {
      ret += coordinatesToWKB(position) + ",";
    }
    return ret.substring(0, ret.length() - 1) + ")";
  }

  public static String geometryToWKB(XyzGeometry geometry) {
    String wkbString = geometry.getClass().getSimpleName().toUpperCase();

    if (geometry instanceof XyzPoint) {
      wkbString += coordinatesToWKB(new ArrayList<>(Arrays.asList(new Position(
          ((XyzPoint) geometry).getCoordinates().getLongitude(),
          ((XyzPoint) geometry).getCoordinates().getLatitude(),
          ((XyzPoint) geometry).getCoordinates().getAltitude() == null
              ? 0
              : ((XyzPoint) geometry).getCoordinates().getAltitude()))));
    } else if (geometry instanceof XyzLineString) {
      wkbString += coordinatesToWKB(((XyzLineString) geometry).getCoordinates());
    } else if (geometry instanceof XyzPolygon) {
      wkbString += linearRingsToWKB(((XyzPolygon) geometry).getCoordinates());
    } else if (geometry instanceof XyzMultiPoint
        || geometry instanceof XyzMultiLineString
        || geometry instanceof XyzMultiPolygon) {
      wkbString += "(";
      if (geometry instanceof XyzMultiPoint) {
        for (Position coordinates : ((XyzMultiPoint) geometry).getCoordinates()) {
          wkbString += coordinatesToWKB(new ArrayList<>(Arrays.asList(new Position(
                  coordinates.getLongitude(),
                  coordinates.getLatitude(),
                  coordinates.getAltitude() == null ? 0 : coordinates.getAltitude()))))
              + ",";
        }
      } else if (geometry instanceof XyzMultiLineString) {
        for (LineStringCoordinates coordinates : ((XyzMultiLineString) geometry).getCoordinates()) {
          wkbString += coordinatesToWKB(coordinates) + ",";
        }
      } else if (geometry instanceof XyzMultiPolygon) {
        for (PolygonCoordinates coordinates : ((XyzMultiPolygon) geometry).getCoordinates()) {
          wkbString += linearRingsToWKB(coordinates) + ",";
        }
      }
      wkbString = wkbString.substring(0, wkbString.length() - 1) + ")";
    }
    return wkbString;
  }
}
