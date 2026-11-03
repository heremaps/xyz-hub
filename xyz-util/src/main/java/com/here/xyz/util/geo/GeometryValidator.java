/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

import com.here.xyz.models.filters.SpatialFilter;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.MultiPointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.coordinates.PositionList;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.GeometryCollection;
import com.here.xyz.models.geojson.implementation.GeometryItem;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;

import com.here.xyz.util.service.BaseConfig;
import com.here.xyz.util.service.BaseHttpServerVerticle;
import java.util.List;

/**
 * Validates the geometry of a spatial filter. All checks work on the GeoJSON model in a single linear pass each, without a conversion
 * to JTS, a projection or a buffer, so that they are cheap enough to run on an event-loop thread.
 */
public class GeometryValidator {
  /**
   * The default maximum number of positions of a spatial filter geometry, see {@link BaseConfig#MAX_SPATIAL_FILTER_COORDINATES}.
   */
  public static final int MAX_NUMBER_OF_COORDNIATES = 12_000;
  private static final double EARTH_MEAN_RADIUS_METERS = 6_371_008.8;
  private static final List<Position> canonical = List.of(
          new Position(-180.0, -90.0),
          new Position(180.0, -90.0),
          new Position(180.0, 90.0),
          new Position(-180.0, 90.0)
  ); // Define the canonical world bbox positions (counter-clockwise)

  /**
   * @return The configured maximum number of positions of a spatial filter geometry, or {@link #MAX_NUMBER_OF_COORDNIATES} if no
   *  configuration is loaded.
   */
  public static int maxCoordinates() {
    return BaseConfig.instance == null ? MAX_NUMBER_OF_COORDNIATES : BaseConfig.instance.MAX_SPATIAL_FILTER_COORDINATES;
  }

  /**
   * Counts the positions of a geometry, summed over all parts and rings, including the items of a GeometryCollection.
   * The count adds up the sizes of the position lists, so its cost grows with the number of rings and parts, not with the positions.
   *
   * @param geometry The geometry, may be null
   * @return The number of positions, 0 for null or missing coordinates
   */
  public static long countCoordinates(Geometry geometry) {
    if (geometry instanceof GeometryCollection collection) {
      long count = 0;
      if (collection.getGeometries() != null)
        for (GeometryItem item : collection.getGeometries())
          count += countCoordinates(item);
      return count;
    }
    return geometry instanceof GeometryItem item ? countPositions(item.getCoordinates()) : 0;
  }

  private static long countPositions(Object coordinates) {
    //Position before List, a Position is a List<Double>
    if (coordinates instanceof Position)
      return 1;
    if (coordinates instanceof PositionList || coordinates instanceof MultiPointCoordinates)
      return ((List<?>) coordinates).size();
    if (coordinates instanceof List<?> parts) {
      long count = 0;
      for (Object part : parts)
        count += countPositions(part);
      return count;
    }
    //null or unknown
    return 0;
  }

  /**
   * Rejects a geometry with more positions than the configured maximum, see {@link #maxCoordinates()}.
   */
  public static void validateCoordinateCount(Geometry geometry) throws GeometryException {
    validateCoordinateCount(geometry, maxCoordinates());
  }

  /**
   * Rejects a geometry with more positions than the given maximum.
   */
  public static void validateCoordinateCount(Geometry geometry, int maxCoordinates) throws GeometryException {
    long count = countCoordinates(geometry);
    if (count > maxCoordinates)
      throw new GeometryException(String.format(
          "Invalid arguments! The filter geometry has %d coordinates, more than the maximum of %d.", count, maxCoordinates));
  }

  /**
   * Validates a spatial filter geometry against the configured maximum number of positions, see {@link #maxCoordinates()}.
   *
   * @see #validateGeometry(Geometry, int, int)
   */
  public static void validateGeometry(Geometry geometry, int radius) throws GeometryException {
    validateGeometry(geometry, radius, maxCoordinates());
  }

  /**
   * Validates a spatial filter geometry: it must not be null, must not have more than <code>maxCoordinates</code> positions, must be a
   * valid GeoJSON geometry and, if it is not a Point and the radius is positive, its buffer must not cross the antimeridian.
   * The position count runs first, so that an oversized geometry is rejected before any other work.
   *
   * @param geometry The filter geometry
   * @param radius The radius in meters by which the filter gets buffered
   * @param maxCoordinates The maximum number of positions
   * @throws GeometryException if the geometry is not acceptable as a spatial filter
   */
  public static void validateGeometry(Geometry geometry, int radius, int maxCoordinates) throws GeometryException {
    if (geometry == null)
      throw new GeometryException("Invalid arguments! Geometry cant be null!");

    validateCoordinateCount(geometry, maxCoordinates);

    try {
      geometry.validate();
    }
    catch (InvalidGeometryException | RuntimeException e) {
      throw new GeometryException("Invalid filter geometry!" + (e.getMessage() == null ? "" : " " + e.getMessage()));
    }

    if (radius > 0 && !(geometry instanceof Point) && crossesAntimeridian(geometry, radius))
      throw new GeometryException("Invalid arguments! geometry filter intersects with antimeridian!");
  }

  /**
   * Checks whether the geometry, buffered by the radius, crosses the antimeridian. The envelope of the GeoJSON positions is expanded
   * by the radius converted to degrees, using the earth's mean radius and the cosine of the expanded envelope's highest absolute
   * latitude. The expansion overestimates the buffer slightly, so the check errs toward reporting a crossing. A buffer that reaches a
   * pole covers every longitude and counts as crossing.
   *
   * @param geometry The geometry with valid positions
   * @param radius The radius in meters, a value &lt;= 0 means no buffer
   * @return true, if the expanded envelope extends beyond -180 or +180 degrees longitude, or if the buffer reaches a pole
   */
  public static boolean crossesAntimeridian(Geometry geometry, int radius) {
    //minLon, minLat, maxLon, maxLat
    double[] envelope = {POSITIVE_INFINITY, POSITIVE_INFINITY, NEGATIVE_INFINITY, NEGATIVE_INFINITY};
    expandEnvelope(geometry, envelope);
    //No positions
    if (envelope[0] > envelope[2])
      return false;

    double dLat = radius > 0 ? Math.toDegrees(radius / EARTH_MEAN_RADIUS_METERS) : 0;
    double maxAbsLat = Math.max(Math.abs(envelope[1]), Math.abs(envelope[3])) + dLat;
    if (dLat > 0 && maxAbsLat >= 90)
      return true;

    double dLon = dLat == 0 ? 0 : dLat / Math.cos(Math.toRadians(maxAbsLat));
    return envelope[0] - dLon < -180 || envelope[2] + dLon > 180;
  }

  private static void expandEnvelope(Geometry geometry, double[] envelope) {
    if (geometry instanceof GeometryCollection collection) {
      if (collection.getGeometries() != null)
        for (GeometryItem item : collection.getGeometries())
          expandEnvelope(item, envelope);
    }
    else if (geometry instanceof GeometryItem item)
      expandEnvelopeByPositions(item.getCoordinates(), envelope);
  }

  private static void expandEnvelopeByPositions(Object coordinates, double[] envelope) {
    //Position before List, a Position is a List<Double>
    if (coordinates instanceof Position position) {
      if (position.size() < 2 || position.get(0) == null || position.get(1) == null)
        return;
      double lon = position.get(0);
      double lat = position.get(1);
      envelope[0] = Math.min(envelope[0], lon);
      envelope[1] = Math.min(envelope[1], lat);
      envelope[2] = Math.max(envelope[2], lon);
      envelope[3] = Math.max(envelope[3], lat);
    }
    else if (coordinates instanceof List<?> parts)
      for (Object part : parts)
        expandEnvelopeByPositions(part, envelope);
  }

  public static void validateSpatialFilter(SpatialFilter spatialFilter) throws BaseHttpServerVerticle.ValidationException {
    try {
      GeometryValidator.validateGeometry(spatialFilter.getGeometry(), spatialFilter.getRadius());
    }
    catch (GeometryException e){
      throw new BaseHttpServerVerticle.ValidationException(e.getMessage());
    }
  }

  public static boolean isWorldBoundingBox(Geometry geometry) {
    if (!(geometry instanceof Polygon inputPolygon)) {
      return false;
    }

    PolygonCoordinates inputCoords = inputPolygon.getCoordinates();

    if (inputCoords == null || inputCoords.isEmpty()) {
      return false;
    }

    LinearRingCoordinates inputLrc = inputCoords.get(0);
    if (inputLrc.size() != 5) {
      return false;
    }

    // Try all 4 valid cyclic permutations
    for (int offset = 0; offset < 4; offset++) {
      boolean match = true;
      for (int i = 0; i <= 4; i++) {
        Position expected = canonical.get((i + offset) % 4);
        // Last point should always match the first to close the ring
        if (i == 4) {
          expected = canonical.get(offset % 4);
        }
        Position actual = inputLrc.get(i);
        if (!expected.equals(actual)) {
          match = false;
          break;
        }
      }
      if (match) {
        return true;
      }
    }

    return false;
  }

  public static class GeometryException extends Exception {
    public GeometryException(String message) {
      super(message);
    }
  }
}
