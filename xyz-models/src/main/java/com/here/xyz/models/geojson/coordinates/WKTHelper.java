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

package com.here.xyz.models.geojson.coordinates;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.LineString;
import com.here.xyz.models.geojson.implementation.MultiLineString;
import com.here.xyz.models.geojson.implementation.MultiPoint;
import com.here.xyz.models.geojson.implementation.MultiPolygon;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;

public class WKTHelper {

  private static String coordinatesToWKB(List<Position> postionList) {
    String ret = "(";
    for (Position position : postionList) {
      ret += position.getLongitude() +" "+position.getLatitude()+" "+(position.getAltitude()  == null ? "" : position.getAltitude())+",";
    }
    return ret.substring(0,ret.length()-1)+")";
  }

  private static String linearRingsToWKB(List<LinearRingCoordinates> coordinates) {
    String ret = "(";
    for (LinearRingCoordinates position : coordinates) {
      ret += coordinatesToWKB(position)+",";
    }
    return ret.substring(0,ret.length()-1)+")";
  }

  public static String geometryToWKB(Geometry geometry) {
    String wkbString = geometry.getClass().getSimpleName().toUpperCase();

    if(geometry instanceof Point) {
      wkbString += coordinatesToWKB(new ArrayList<>(Arrays.asList(new Position(((Point)geometry).getCoordinates().getLongitude(),
          ((Point)geometry).getCoordinates().getLatitude(),
          ((Point)geometry).getCoordinates().getAltitude() == null ? 0 :((Point)geometry).getCoordinates().getAltitude()
       ))));
    }else if(geometry instanceof LineString) {
      wkbString += coordinatesToWKB(((LineString)geometry).getCoordinates());
    }else if(geometry instanceof Polygon) {
      wkbString += linearRingsToWKB(((Polygon)geometry).getCoordinates());
    }else if(geometry instanceof MultiPoint || geometry instanceof MultiLineString || geometry instanceof MultiPolygon){
      wkbString += "(";
      if(geometry instanceof MultiPoint) {
        for(Position coordinates : ((MultiPoint)geometry).getCoordinates()) {
          wkbString += coordinatesToWKB(new ArrayList<>(Arrays.asList(new Position(coordinates.getLongitude(),
              coordinates.getLatitude(),
              coordinates.getAltitude() == null ? 0 : coordinates.getAltitude()
           ))))+",";
        }
      }else if(geometry instanceof MultiLineString) {
        for(LineStringCoordinates coordinates : ((MultiLineString)geometry).getCoordinates()) {
          wkbString += coordinatesToWKB(coordinates)+",";
        }
      }else if(geometry instanceof MultiPolygon) {
        for(PolygonCoordinates coordinates : ((MultiPolygon)geometry).getCoordinates()) {
          wkbString += linearRingsToWKB(coordinates)+",";
        }
      }
      wkbString = wkbString.substring(0,wkbString.length()-1)+")";
    }
    return wkbString;
  }
}
