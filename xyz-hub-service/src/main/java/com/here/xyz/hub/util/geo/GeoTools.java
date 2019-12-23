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

package com.here.xyz.hub.util.geo;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.operation.polygonize.Polygonizer;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.NoSuchAuthorityCodeException;
import org.opengis.referencing.crs.CRSAuthorityFactory;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

/**
 * A helper class to do certain things with JTS.
 */
public class GeoTools {

  /**
   * The WGS'84 coordinate reference system.
   */
  public static final String WGS84_EPSG = "EPSG:4326";
  /**
   * The Google Mercator coordinate reference system, which basically uses meters from -20037508.342789244 to + 20037508.342789244.
   */
  public static final String WEB_MERCATOR_EPSG = "EPSG:3857";
  /**
   * Equal-Area Scalable Earth Grid coordinate reference system.
   *
   * @see <a href="http://nsidc.org/data/ease/ease_grid.html">http://nsidc.org/data/ease/ease_grid.html</a>
   */
  public static final String EASE_GRID_EPSG = "EPSG:3410";
  /**
   * Difference to the min and max latitude.
   */
  protected static final double WEB_EPSILON = 0.0001d;
  /**
   * The minimal latitude value to be allowed for mathematical operations to succeed.
   */
  protected static final double WEB_MIN_LATITUDE = -90d + WEB_EPSILON;
  /**
   * The maximal latitude value to be allowed for mathematical operations to succeed.
   */
  protected static final double WEB_MAX_LATITUDE = +90d - WEB_EPSILON;
  /**
   * The minimal longitude to be allowed for mathematical operations to succeed.
   */
  protected static final double WEB_MIN_LONGITUDE = -180d;
  /**
   * The maximal longitude to be allowed for mathematical operations to succeed.
   */
  protected static final double WEB_MAX_LONGITUDE = +180d;
  /**
   * We use this factory to guarantee that the coordinate order is x, y (so longitude/latitude) and not in an unknown state.
   */
  private static final CRSAuthorityFactory factory = CRS.getAuthorityFactory(true); // first parameter is "longitudeFirst"
  private static final ConcurrentHashMap<String, CoordinateReferenceSystem> crsCache = new ConcurrentHashMap<>();
  private static final ConcurrentHashMap<String, MathTransform> transformCache = new ConcurrentHashMap<>();

  /**
   * Returns the cached coordinate reference system for the given EPSG identifier.
   *
   * @param crsId the CRS identifier of the coordinate reference system.
   * @return the coordinate reference system.
   * @throws NullPointerException if the given epsgId is null.
   * @throws NoSuchAuthorityCodeException if the given EPSG identifier is unknown.
   * @throws FactoryException if the requested coordinate reference system can't be created.
   */
  public static final CoordinateReferenceSystem crs(final String crsId)
      throws NullPointerException, NoSuchAuthorityCodeException, FactoryException {
    if (crsId == null) {
      throw new NullPointerException("crsId");
    }
    if (crsCache.containsKey(crsId)) {
      return crsCache.get(crsId);
    }
    final CoordinateReferenceSystem newCRS = factory.createCoordinateReferenceSystem(crsId);
    final CoordinateReferenceSystem existingCRS = crsCache.putIfAbsent(crsId, newCRS);
    if (existingCRS != null) {
      return existingCRS;
    }
    return newCRS;
  }

  /**
   * Returns a mathematical transformation from the first given EPSG coordinate reference system into the second one. This method can be
   * used in conjunction with the {@link JTS#transform(Geometry, MathTransform)} method.
   *
   * @param fromCrsId the CRS identifier of the source coordinate reference system.
   * @param toCrsId the CRS identifier of the destination coordinate reference system.
   * @throws NullPointerException if any of the given EPSG identifier is null.
   * @throws NoSuchAuthorityCodeException if any of the given EPSG identifier is unknown.
   * @throws FactoryException if the requested coordinate reference system can't be created or the transformation or the coordinate failed.
   */
  public static final MathTransform mathTransform(final String fromCrsId, final String toCrsId)
      throws NullPointerException, NoSuchAuthorityCodeException, FactoryException {
    if (fromCrsId == null) {
      throw new NullPointerException("fromCrsId");
    }
    if (toCrsId == null) {
      throw new NullPointerException("toCrsId");
    }
    final String id = fromCrsId + ":" + toCrsId;
    if (transformCache.containsKey(id)) {
      return transformCache.get(id);
    }
    final CoordinateReferenceSystem fromCRS = crs(fromCrsId);
    final CoordinateReferenceSystem toCRS = crs(toCrsId);
    final MathTransform newTransform = CRS.findMathTransform(fromCRS, toCRS, true);
    final MathTransform existingTransform = transformCache.putIfAbsent(id, newTransform);
    if (existingTransform != null) {
      return existingTransform;
    }
    return newTransform;
  }

  public static void main(String[] args) throws Exception {
    final PrecisionModel precisionModel = new PrecisionModel(PrecisionModel.FLOATING);
    final GeometryFactory geometryFactory = new GeometryFactory(precisionModel);

    final Point topLeft = geometryFactory.createPoint(new Coordinate(WEB_MIN_LONGITUDE, WEB_MIN_LATITUDE));
    final Point lowerRight = geometryFactory.createPoint(new Coordinate(WEB_MAX_LATITUDE, WEB_MAX_LATITUDE));

    System.out.println("PIXEL:");
    System.out.println(crs(WEB_MERCATOR_EPSG).getCoordinateSystem().getAxis(0).getMinimumValue() + " -> "
        + crs(WEB_MERCATOR_EPSG).getCoordinateSystem().getAxis(0).getMaximumValue());
    System.out.println(crs(WEB_MERCATOR_EPSG).getCoordinateSystem().getAxis(1).getMinimumValue() + " -> "
        + crs(WEB_MERCATOR_EPSG).getCoordinateSystem().getAxis(1).getMaximumValue());

    System.out.println("WGS84:");
    System.out.println(crs(WGS84_EPSG).getCoordinateSystem().getAxis(0).getMinimumValue() + " -> "
        + crs(WGS84_EPSG).getCoordinateSystem().getAxis(0).getMaximumValue());
    System.out.println(crs(WGS84_EPSG).getCoordinateSystem().getAxis(1).getMinimumValue() + " -> "
        + crs(WGS84_EPSG).getCoordinateSystem().getAxis(1).getMaximumValue());

    System.out.println("EASE-GRID:");
    System.out.println(crs(EASE_GRID_EPSG).getCoordinateSystem().getAxis(0).getMinimumValue() + " -> "
        + crs(EASE_GRID_EPSG).getCoordinateSystem().getAxis(0).getMaximumValue());
    System.out.println(crs(EASE_GRID_EPSG).getCoordinateSystem().getAxis(1).getMinimumValue() + " -> "
        + crs(EASE_GRID_EPSG).getCoordinateSystem().getAxis(1).getMaximumValue());

    // String to = EASE_GRID_EPSG;
    String to = WEB_MERCATOR_EPSG;
    Point pixel = (Point) JTS.transform(topLeft, mathTransform(WGS84_EPSG, to));
    System.out.println(((long) pixel.getX()) + ", " + ((long) pixel.getY()));
    pixel = (Point) JTS.transform(lowerRight, mathTransform(WGS84_EPSG, to));
    System.out.println(((long) pixel.getX()) + ", " + ((long) pixel.getY()));
  }

  /**
   * Get / create a valid version of the geometry given. If the geometry is a polygon or multi polygon, self intersections / inconsistencies
   * are fixed. Otherwise the geometry is returned.
   *
   * @return a geometry
   */
  public static Geometry validate(Geometry geom) {
    if (geom instanceof Polygon) {
      if (geom.isValid()) {
        geom.normalize(); // validate does not pick up rings in the wrong order - this will fix that
        return geom; // If the polygon is valid just return it
      }
      Polygonizer polygonizer = new Polygonizer();
      addPolygon((Polygon) geom, polygonizer);
      return toPolygonGeometry(polygonizer.getPolygons(), geom.getFactory());
    } else if (geom instanceof MultiPolygon) {
      if (geom.isValid()) {
        geom.normalize(); // validate does not pick up rings in the wrong order - this will fix that
        return geom; // If the multipolygon is valid just return it
      }
      Polygonizer polygonizer = new Polygonizer();
      for (int n = geom.getNumGeometries(); n-- > 0; ) {
        addPolygon((Polygon) geom.getGeometryN(n), polygonizer);
      }
      return toPolygonGeometry(polygonizer.getPolygons(), geom.getFactory());
    } else {
      return geom; // In my case, I only care about polygon / multipolygon geometries
    }
  }

  /**
   * Add all line strings from the polygon given to the polygonizer given
   *
   * @param polygon polygon from which to extract line strings
   * @param polygonizer polygonizer
   */
  static void addPolygon(Polygon polygon, Polygonizer polygonizer) {
    addLineString(polygon.getExteriorRing(), polygonizer);
    for (int n = polygon.getNumInteriorRing(); n-- > 0; ) {
      addLineString(polygon.getInteriorRingN(n), polygonizer);
    }
  }

  /**
   * Add the linestring given to the polygonizer
   *
   * @param lineString line string
   * @param polygonizer polygonizer
   */
  static void addLineString(LineString lineString, Polygonizer polygonizer) {

    if (lineString instanceof LinearRing) { // LinearRings are treated differently to line strings : we need a LineString NOT a LinearRing
      lineString = lineString.getFactory().createLineString(lineString.getCoordinateSequence());
    }

    // unioning the linestring with the point makes any self intersections explicit.
    Point point = lineString.getFactory().createPoint(lineString.getCoordinateN(0));
    Geometry toAdd = lineString.union(point);

    //Add result to polygonizer
    polygonizer.add(toAdd);
  }

  /**
   * Get a geometry from a collection of polygons.
   *
   * @param polygons collection
   * @param factory factory to generate MultiPolygon if required
   * @return null if there were no polygons, the polygon if there was only one, or a MultiPolygon containing all polygons otherwise
   */
  static Geometry toPolygonGeometry(Collection<Polygon> polygons, GeometryFactory factory) {
    switch (polygons.size()) {
      case 0:
        return null; // No valid polygons!
      case 1:
        return polygons.iterator().next(); // single polygon - no need to wrap
      default:
        //polygons may still overlap! Need to sym difference them
        Iterator<Polygon> iter = polygons.iterator();
        Geometry ret = iter.next();
        while (iter.hasNext()) {
          ret = ret.symDifference(iter.next());
        }
        return ret;
    }
  }
}
