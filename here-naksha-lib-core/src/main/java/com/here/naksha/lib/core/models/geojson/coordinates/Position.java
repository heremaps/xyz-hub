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

import com.here.naksha.lib.core.models.geojson.declaration.IBoundedCoordinates;
import java.util.ArrayList;
import java.util.List;

public class Position extends ArrayList<Double> implements IBoundedCoordinates {

  private static final int LONGITUDE = 0;
  private static final int LATITUDE = 1;
  private static final int ALTITUDE = 2;

  public Position() {
    super();
  }

  public Position(double longitude, double latitude) {
    this();
    this.add(LONGITUDE, longitude);
    this.add(LATITUDE, latitude);
  }

  public Position(double longitude, double latitude, double altitude) {
    this(longitude, latitude);
    this.add(ALTITUDE, altitude);
  }

  /**
   * Converts the given list into a position.
   *
   * @param coordinates a list of two or three numbers.
   * @return the coordinate implementation or null, if the list is null, empty or does not contain
   *     two or three numbers.
   */
  public static Position fromList(final List<?> coordinates) {
    if (coordinates == null) {
      return null;
    }
    final int size = coordinates.size();
    if (size < 2 || size > 3) {
      return null;
    }

    final Object raw0 = coordinates.get(0);
    final Object raw1 = coordinates.get(1);
    final Object raw2;
    if (size == 3) {
      raw2 = coordinates.get(2);
    } else {
      raw2 = null;
    }

    if ((raw0 instanceof Number)
        && //
        (raw1 instanceof Number)
        && //
        ((raw2 == null) || (raw2 instanceof Number)) //
    ) {
      // This is a coordinate.
      final Double longitude = ((Number) raw0).doubleValue();
      final Double latitude = ((Number) raw1).doubleValue();
      final Double altitude;
      if (raw2 == null) {
        altitude = null;
      } else {
        altitude = ((Number) raw2).doubleValue();
      }
      return new Position(longitude, latitude, altitude != null ? altitude : 0d);
    }

    return null;
  }

  public Double getLongitude() {
    return this.get(LONGITUDE);
  }

  public Double setLongitude(Double longitude) {
    Double old = this.remove(LONGITUDE);
    this.add(LONGITUDE, longitude);
    return old;
  }

  public Double getLatitude() {
    return this.get(LATITUDE);
  }

  public Double setLatitude(Double latitude) {
    Double old = this.remove(LATITUDE);
    this.add(LATITUDE, latitude);
    return old;
  }

  public Double getAltitude() {
    return this.size() > 2 ? this.get(ALTITUDE) : null;
  }

  public Double setAltitude(Double altitude) {
    Double old = this.size() > 2 ? this.remove(ALTITUDE) : null;
    this.add(ALTITUDE, altitude);
    return old;
  }

  public BBox calculateBBox() {
    Double lat = getLatitude();
    Double lon = getLongitude();
    if (lat != null && lon != null) {
      return new BBox(lon, lat, lon, lat);
    }
    return null;
  }
}
