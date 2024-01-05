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

package com.here.xyz.models.geojson.coordinates;

import com.here.xyz.models.geojson.implementation.GeometryItem;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;

public class JTSHelper {

  private final static Point[] EMPTY_POINT_ARRAY = new Point[0];

  // ############## Methods to convert from GeoJSON to JTS ##############
  public static GeometryFactory factory = new GeometryFactory(new PrecisionModel(), 4326);

  /**
   * Creates a Point.
   */
  public static Point toPoint(PointCoordinates coords) {
    if (coords == null) {
      return null;
    }

    if (coords.getAltitude() != null) {
      return factory.createPoint(new Coordinate(coords.getLongitude(), coords.getLatitude(), coords.getAltitude()));
    }

    return factory.createPoint(new Coordinate(coords.getLongitude(), coords.getLatitude()));
  }

  /**
   * Creates a MultiPoint.
   */
  public static MultiPoint toMultiPoint(MultiPointCoordinates coords) {
    if (coords == null) {
      return null;
    }

    ArrayList<Point> pointsList = new ArrayList<>();

    for (PointCoordinates pointCoords : coords) {
      pointsList.add(toPoint(pointCoords));
    }

    return factory.createMultiPoint(pointsList.toArray(EMPTY_POINT_ARRAY));
  }

  /**
   * Creates a LineString.
   */
  public static LineString toLineString(LineStringCoordinates coords) {
    if (coords == null) {
      return null;
    }

    Coordinate[] jtsCoords = new Coordinate[coords.size()];

    for (int i = 0; i < jtsCoords.length; i++) {
      jtsCoords[i] = toCoordinate(coords.get(i));
    }

    return JTSHelper.factory.createLineString(jtsCoords);
  }

  /**
   * Creates a MultiLineString.
   */
  public static MultiLineString toMultiLineString(MultiLineStringCoordinates coords) {
    if (coords == null) {
      return null;
    }

    LineString[] lines = new LineString[coords.size()];

    for (int i = 0; i < lines.length; i++) {
      lines[i] = toLineString(coords.get(i));
    }

    return JTSHelper.factory.createMultiLineString(lines);
  }

  /**
   * Creates a Polygon.
   */
  public static Polygon toPolygon(PolygonCoordinates coords) {
    if (coords == null) {
      return null;
    }

    LinearRing shell = toLinearRing(coords.get(0));
    if (coords.size() == 1) {
      return JTSHelper.factory.createPolygon(shell);
    }

    LinearRing[] holes = new LinearRing[coords.size() - 1];
    for (int i = 1; i < coords.size(); i++) {
      holes[i - 1] = toLinearRing(coords.get(i));
    }

    return JTSHelper.factory.createPolygon(shell, holes);
  }

  /**
   * Creates a MultiPolygon.
   */
  public static MultiPolygon toMultiPolygon(MultiPolygonCoordinates coords) {
    if (coords == null) {
      return null;
    }

    Polygon[] polygons = new Polygon[coords.size()];

    for (int i = 0; i < polygons.length; i++) {
      polygons[i] = toPolygon(coords.get(i));
    }

    return JTSHelper.factory.createMultiPolygon(polygons);
  }

  /**
   * Creates a linear ring.
   */
  public static LinearRing toLinearRing(LinearRingCoordinates coords) {
    if (coords == null) {
      return null;
    }

    Coordinate[] jtsCoords = new Coordinate[coords.size()];

    for (int i = 0; i < jtsCoords.length; i++) {
      jtsCoords[i] = toCoordinate(coords.get(i));
    }

    return factory.createLinearRing(jtsCoords);
  }

  /**
   * Creates a JTS Coordinate.
   */
  public static Coordinate toCoordinate(Position pos) {
    return (pos.getAltitude() == null)
        ? new Coordinate(pos.getLongitude(), pos.getLatitude()) : new Coordinate(pos.getLongitude(), pos.getLatitude(), pos.getAltitude());
  }

  /**
   * Creates a JTS Geometry from the provided GeoJSON geometry.
   */
  @SuppressWarnings("unchecked")
  public static <X extends Geometry> X toGeometry(com.here.xyz.models.geojson.implementation.Geometry geometry) {
    if (geometry == null) {
      return null;
    }

    if (geometry instanceof com.here.xyz.models.geojson.implementation.Point) {
      return (X) toPoint(((com.here.xyz.models.geojson.implementation.Point) geometry).getCoordinates());
    }
    if (geometry instanceof com.here.xyz.models.geojson.implementation.MultiPoint) {
      return (X) toMultiPoint(((com.here.xyz.models.geojson.implementation.MultiPoint) geometry).getCoordinates());
    }
    if (geometry instanceof com.here.xyz.models.geojson.implementation.LineString) {
      return (X) toLineString(((com.here.xyz.models.geojson.implementation.LineString) geometry).getCoordinates());
    }
    if (geometry instanceof com.here.xyz.models.geojson.implementation.MultiLineString) {
      return (X) toMultiLineString(((com.here.xyz.models.geojson.implementation.MultiLineString) geometry).getCoordinates());
    }
    if (geometry instanceof com.here.xyz.models.geojson.implementation.Polygon) {
      return (X) toPolygon(((com.here.xyz.models.geojson.implementation.Polygon) geometry).getCoordinates());
    }
    if (geometry instanceof com.here.xyz.models.geojson.implementation.MultiPolygon) {
      return (X) toMultiPolygon(((com.here.xyz.models.geojson.implementation.MultiPolygon) geometry).getCoordinates());
    }
    if (geometry instanceof com.here.xyz.models.geojson.implementation.GeometryCollection) {
      return (X) toGeometryCollection(((com.here.xyz.models.geojson.implementation.GeometryCollection) geometry));
    }
    return null;
  }

  public static GeometryCollection toGeometryCollection(com.here.xyz.models.geojson.implementation.GeometryCollection geometryCollection) {
    if (geometryCollection == null) {
      return null;
    }

    List<GeometryItem> geometries = geometryCollection.getGeometries();
    int len = geometries.size();
    Geometry[] jtsGeometries = new Geometry[len];

    for (int i = 0; i < len; i++) {
      jtsGeometries[i] = toGeometry(geometries.get(i));
    }

    return new GeometryCollection(jtsGeometries, factory);

  }

  // ############## Methods to convert from JTS to GeoJSON ##############

  /**
   * Create a GeoJSON position.
   */
  public static Position createPosition(Coordinate coord) {
    return (Double.isNaN(coord.z))
        ? new Position(coord.x, coord.y) : new Position(coord.x, coord.y, coord.z);
  }

  /**
   * Create GeoJSON Point coordinates.
   */
  public static PointCoordinates createPointCoordinates(Point geom) {
    if (geom == null) {
      return null;
    }
    final Coordinate coordinate = geom.getCoordinate();

    return (Double.isNaN(coordinate.z))
        ? new PointCoordinates(coordinate.x, coordinate.y) : new PointCoordinates(coordinate.x, coordinate.y, coordinate.z);
  }

  /**
   * Create GeoJSON MultiPoint coordinates.
   */
  public static MultiPointCoordinates createMultiPointCoordinates(MultiPoint geom) {
    if (geom == null) {
      return null;
    }

    int len = geom.getNumGeometries();
    MultiPointCoordinates multiPointCoordinates = new MultiPointCoordinates(len);

    for (int i = 0; i < len; i++) {
      multiPointCoordinates.add(createPointCoordinates((Point) geom.getGeometryN(i)));
    }

    return multiPointCoordinates;
  }

  /**
   * Create GeoJSON LineString coordinates.
   */
  public static LineStringCoordinates createLineStringCoordinates(LineString geom) {
    if (geom == null) {
      return null;
    }

    int len = geom.getNumPoints();
    LineStringCoordinates lineStringCoordinates = new LineStringCoordinates(len);

    for (int i = 0; i < len; i++) {
      lineStringCoordinates.add(createPosition(geom.getCoordinateN(i)));
    }

    return lineStringCoordinates;
  }

  /**
   * Create GeoJSON MultiLineString coordinates.
   */
  public static MultiLineStringCoordinates createMultiLineStringCoordinates(MultiLineString geom) {
    if (geom == null) {
      return null;
    }

    int len = geom.getNumGeometries();
    MultiLineStringCoordinates multiLineStringCoordinates = new MultiLineStringCoordinates(len);

    for (int i = 0; i < len; i++) {
      multiLineStringCoordinates.add(createLineStringCoordinates((LineString) geom.getGeometryN(i)));
    }

    return multiLineStringCoordinates;
  }

  /**
   * Create GeoJSON LinearRing coordinates.
   */
  public static LinearRingCoordinates createLinearRingCoordinates(LinearRing geom) {
    if (geom == null) {
      return null;
    }

    int len = geom.getNumPoints();
    LinearRingCoordinates linearRingCoordinates = new LinearRingCoordinates(len);

    for (int i = 0; i < len; i++) {
      linearRingCoordinates.add(createPosition(geom.getCoordinateN(i)));
    }

    return linearRingCoordinates;
  }

  /**
   * Create GeoJSON Polygon coordinates.
   */
  public static PolygonCoordinates createPolygonCoordinates(Polygon geom) {
    if (geom == null) {
      return null;
    }

    LinearRing shell = (LinearRing) geom.getExteriorRing();
    int lenInterior = geom.getNumInteriorRing();

    PolygonCoordinates polygonCoordinates = new PolygonCoordinates(lenInterior + 1);
    polygonCoordinates.add(createLinearRingCoordinates(shell));

    for (int i = 0; i < lenInterior; i++) {
      polygonCoordinates.add(createLinearRingCoordinates((LinearRing) geom.getInteriorRingN(i)));
    }

    return polygonCoordinates;
  }

  /**
   * Create MultiPolygon coordinates.
   */
  public static MultiPolygonCoordinates createMultiPolygonCoordinates(MultiPolygon geom) {
    if (geom == null) {
      return null;
    }

    int len = geom.getNumGeometries();
    MultiPolygonCoordinates multiPolygonCoordinates = new MultiPolygonCoordinates(len);

    for (int i = 0; i < len; i++) {
      multiPolygonCoordinates.add(createPolygonCoordinates((Polygon) geom.getGeometryN(i)));
    }

    return multiPolygonCoordinates;
  }

  @SuppressWarnings("unchecked")
  public static <X extends com.here.xyz.models.geojson.implementation.Geometry> X fromGeometry(Geometry jtsGeometry) {
    if (jtsGeometry == null) {
      return null;
    }

    if (jtsGeometry instanceof Point) {
      return (X) new com.here.xyz.models.geojson.implementation.Point().withCoordinates(createPointCoordinates((Point) jtsGeometry));
    }
    if (jtsGeometry instanceof MultiPoint) {
      return (X) new com.here.xyz.models.geojson.implementation.MultiPoint()
          .withCoordinates(createMultiPointCoordinates((MultiPoint) jtsGeometry));
    }
    if (jtsGeometry instanceof LineString) {
      return (X) new com.here.xyz.models.geojson.implementation.LineString()
          .withCoordinates(createLineStringCoordinates((LineString) jtsGeometry));
    }
    if (jtsGeometry instanceof MultiLineString) {
      return (X) new com.here.xyz.models.geojson.implementation.MultiLineString()
          .withCoordinates(createMultiLineStringCoordinates((MultiLineString) jtsGeometry));
    }
    if (jtsGeometry instanceof Polygon) {
      return (X) new com.here.xyz.models.geojson.implementation.Polygon().withCoordinates(createPolygonCoordinates((Polygon) jtsGeometry));
    }
    if (jtsGeometry instanceof MultiPolygon) {
      return (X) new com.here.xyz.models.geojson.implementation.MultiPolygon()
          .withCoordinates(createMultiPolygonCoordinates((MultiPolygon) jtsGeometry));
    }
    if (jtsGeometry instanceof GeometryCollection) {
      return (X) fromGeometryCollection((GeometryCollection) jtsGeometry);
    }

    return null;
  }

  public static com.here.xyz.models.geojson.implementation.GeometryCollection fromGeometryCollection(
      GeometryCollection jtsGeometryCollection) {
    if (jtsGeometryCollection == null) {
      return null;
    }

    com.here.xyz.models.geojson.implementation.GeometryCollection geometryCollection = new com.here.xyz.models.geojson.implementation.GeometryCollection();

    int len = jtsGeometryCollection.getNumGeometries();
    List<com.here.xyz.models.geojson.implementation.GeometryItem> geometries = new ArrayList<>();

    for (int i = 0; i < len; i++) {
      geometries.add(fromGeometry(jtsGeometryCollection.getGeometryN(i)));
    }

    return geometryCollection.withGeometries(geometries);
  }
}
