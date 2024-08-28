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

  private static String coordinatesToWKT(List<Position> postionList) {
    String ret = "(";
    for (Position position : postionList) {
      ret += position.getLongitude() +" "+position.getLatitude()+" "+(position.getAltitude()  == null ? "" : position.getAltitude())+",";
    }
    return ret.substring(0,ret.length()-1)+")";
  }

  private static String linearRingsToWKT(List<LinearRingCoordinates> coordinates) {
    String ret = "(";
    for (LinearRingCoordinates position : coordinates) {
      ret += coordinatesToWKT(position)+",";
    }
    return ret.substring(0,ret.length()-1)+")";
  }

  @Deprecated
  public static String geometryToWKB(Geometry geometry) { return geometryToWKT3d(geometry); }

  public static String geometryToWKT2d(Geometry geometry) 
  { 
    org.locationtech.jts.geom.Geometry g = geometry.getJTSGeometry();
    String wktString = g.toText(); 
    return wktString;
  }

  public static String geometryToWKT3d(Geometry geometry) {
    
    String wktString = geometry.getClass().getSimpleName().toUpperCase();

    if(geometry instanceof Point) {
      wktString += coordinatesToWKT(new ArrayList<>(Arrays.asList(new Position(((Point)geometry).getCoordinates().getLongitude(),
          ((Point)geometry).getCoordinates().getLatitude(),
          ((Point)geometry).getCoordinates().getAltitude() == null ? 0 :((Point)geometry).getCoordinates().getAltitude()
       ))));
    }else if(geometry instanceof LineString) {
      wktString += coordinatesToWKT(((LineString)geometry).getCoordinates());
    }else if(geometry instanceof Polygon) {
      wktString += linearRingsToWKT(((Polygon)geometry).getCoordinates());
    }else if(geometry instanceof MultiPoint || geometry instanceof MultiLineString || geometry instanceof MultiPolygon){
      wktString += "(";
      if(geometry instanceof MultiPoint) {
        for(Position coordinates : ((MultiPoint)geometry).getCoordinates()) {
          wktString += coordinatesToWKT(new ArrayList<>(Arrays.asList(new Position(coordinates.getLongitude(),
              coordinates.getLatitude(),
              coordinates.getAltitude() == null ? 0 : coordinates.getAltitude()
           ))))+",";
        }
      }else if(geometry instanceof MultiLineString) {
        for(LineStringCoordinates coordinates : ((MultiLineString)geometry).getCoordinates()) {
          wktString += coordinatesToWKT(coordinates)+",";
        }
      }else if(geometry instanceof MultiPolygon) {
        for(PolygonCoordinates coordinates : ((MultiPolygon)geometry).getCoordinates()) {
          wktString += linearRingsToWKT(coordinates)+",";
        }
      }
      wktString = wktString.substring(0,wktString.length()-1)+")";
    }
    return wktString;
  }
}
