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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.exceptions.InvalidGeometryException;
import com.here.naksha.lib.core.util.json.JsonObject;
import java.util.List;

/** Implemented following: https://tools.ietf.org/html/rfc7946#section-3.1 */
@JsonSubTypes({
  @JsonSubTypes.Type(value = XyzGeometryCollection.class, name = "GeometryCollection"),
  @JsonSubTypes.Type(value = XyzGeometryItem.class)
})
public abstract class XyzGeometry extends JsonObject implements Typed {

  public static final String BBOX = "bbox";

  @JsonProperty(BBOX)
  @JsonInclude(Include.NON_NULL)
  private BBox bbox;

  @JsonIgnore
  private org.locationtech.jts.geom.Geometry geomCache;

  /**
   * Convert a JTS geometry into a {@link XyzGeometry Geo JSON geometry}.
   *
   * @param jtsGeometry the JTS geometry to convert.
   * @param <T> the type of the Geo JSON geometry.
   * @return the Geo JSON geometry or null, if conversion is not possible or results in null.
   */
  @SuppressWarnings({"unchecked", "unused"})
  public static <T extends XyzGeometry> T convertJTSGeometry(org.locationtech.jts.geom.Geometry jtsGeometry) {
    if (jtsGeometry == null) {
      return null;
    }

    return (T) JTSHelper.fromGeometry(jtsGeometry);
  }

  /**
   * A helper method that validates the given object as GeoJSON Point coordinates. When the method
   * returns, raw is guaranteed to be a List&lt;Number&gt;.
   *
   * @param raw the object to test.
   * @throws InvalidGeometryException if the given object does not reflect Point coordinates.
   */
  public static void validatePointCoordinates(final Object raw) throws InvalidGeometryException {
    if (!(raw instanceof List)) {
      throw new InvalidGeometryException("Coordinates are expected to be an array");
    }
    @SuppressWarnings("unchecked")
    final List<Object> values = (List<Object>) raw;
    if (values.size() < 2) {
      throw new InvalidGeometryException(
          "A coordinates array of a Point must contain at least [longitude, latitude], but its size is less than 2");
    }
    if (values.size() > 3) {
      throw new InvalidGeometryException(
          "A coordinates array of a Point must contain maximal [longitude, latitude, altitude], but it contains more elements");
    }
    final Object rawLon = values.get(0);
    final Object rawLat = values.get(1);
    if (!(rawLon instanceof Number)) {
      throw new InvalidGeometryException(
          "The longitude (1st) value in coordinates of the Point is no valid number");
    }
    if (!(rawLat instanceof Number)) {
      throw new InvalidGeometryException(
          "The latitude (2nd) value in coordinates of the Point is no valid number");
    }
    final double lon = ((Number) rawLon).doubleValue();
    final double lat = ((Number) rawLat).doubleValue();
    if (lon < -180d || lon > +180d) {
      throw new InvalidGeometryException(
          "The longitude (1st) value in coordinates of the Point is out of bounds (< -180 or > +180)");
    }
    if (lat < -90d || lat > +90d) {
      throw new InvalidGeometryException(
          "The latitude (2nd) value in coordinates of the Point is out of bounds (< -90 or > +90)");
    }
    if (values.size() == 3) {
      final Object rawAlt = values.get(2);
      if (!(rawAlt instanceof Number)) {
        throw new InvalidGeometryException(
            "The altitude (3rst) value in coordinates of the Point is no valid number");
      }
    }
  }

  /**
   * A helper method that validates the given object as GeoJSON MultiPoint coordinates.
   *
   * @param raw the object to test.
   * @throws InvalidGeometryException if the given object does not reflect MultiPoint coordinates.
   */
  public static void validateMultiPointCoordinates(final Object raw) throws InvalidGeometryException {
    if (!(raw instanceof List)) {
      throw new InvalidGeometryException("Coordinates are expected to be an array");
    }
    @SuppressWarnings("unchecked")
    final List<Object> points = (List<Object>) raw;
    for (int i = 0; i < points.size(); i++) {
      final Object point = points.get(i);
      if (!(point instanceof List)) {
        throw new InvalidGeometryException("Expected Point at index #" + i + " in the MultiPoint coordinates");
      }
      validatePointCoordinates(point);
    }
  }

  /**
   * A helper method that validates the given object as GeoJSON LineString coordinates.
   *
   * @param raw the object to test.
   * @throws InvalidGeometryException if the given object does not reflect LineString coordinates.
   */
  public static void validateLineStringCoordinates(final Object raw) throws InvalidGeometryException {
    if (!(raw instanceof List)) {
      throw new InvalidGeometryException("Coordinates are expected to be an array");
    }
    @SuppressWarnings("unchecked")
    final List<Object> points = (List<Object>) raw;
    if (points.size() < 2) {
      throw new InvalidGeometryException("Coordinates of a LineString must have at least two positions");
    }
    for (int i = 0; i < points.size(); i++) {
      final Object point = points.get(i);
      if (!(point instanceof List)) {
        throw new InvalidGeometryException("Expected Point at index #" + i + " in the LineString coordinates");
      }
      validatePointCoordinates(point);
    }
  }

  /**
   * A helper method that validates the given object as GeoJSON MultiLineString coordinates.
   *
   * @param raw the object to test.
   * @throws InvalidGeometryException if the given object does not reflect MultiLineString
   *     coordinates.
   */
  public static void validateMultiLineStringCoordinates(final Object raw) throws InvalidGeometryException {
    if (!(raw instanceof List)) {
      throw new InvalidGeometryException("Coordinates are expected to be an array");
    }
    @SuppressWarnings("unchecked")
    final List<Object> lineStrings = (List<Object>) raw;
    for (int i = 0; i < lineStrings.size(); i++) {
      final Object lineString = lineStrings.get(i);
      if (!(lineString instanceof List)) {
        throw new InvalidGeometryException(
            "Expected LineString at index #" + i + " of the MultiLineString coordinates");
      }
      validateLineStringCoordinates(lineString);
    }
  }

  /**
   * A helper method that validates the given object as GeoJSON LinearRing coordinates.
   *
   * @param raw the object to test.
   * @param isExterior if the ring is the exterior ring (the first element in the parent array);
   *     when false then this is an interior ring.
   * @throws InvalidGeometryException if the given object does not reflect LinearRing coordinates.
   */
  public static void validateLinearRingCoordinates(final Object raw, boolean isExterior)
      throws InvalidGeometryException {
    if (!(raw instanceof List)) {
      throw new InvalidGeometryException("Coordinates are expected to be an array");
    }
    final List<Object> points = (List<Object>) raw;
    if (points.size() < 4) {
      throw new InvalidGeometryException("Coordinates of a LinearRing must have at least four positions");
    }
    //
    // Note: A linear ring MUST follow the right-hand rule with respect to the area it bounds, i.e.,
    // exterior rings are
    // counterclockwise, and holes are clockwise.
    //
    // But: Note: the [GJ2008] specification did not discuss linear ring winding order. For
    // backwards compatibility, parsers SHOULD NOT
    // reject Polygons that do not follow the right-hand rule.
    //
    // --> Therefore we will not check the right-hand rule here, even while it would be possible!
    for (int i = 0; i < points.size(); i++) {
      final Object point = points.get(i);
      if (!(point instanceof List)) {
        throw new InvalidGeometryException("Expected Point at index #" + i + " in the LinearRing coordinates");
      }
      validatePointCoordinates(point);
    }
    final List<Number> firstPoint = (List<Number>) points.get(0);
    final List<Number> lastPoint = (List<Number>) points.get(points.size() - 1);
    if (firstPoint.size() != lastPoint.size()
        || //
        firstPoint.get(0).doubleValue() != lastPoint.get(0).doubleValue()
        || //
        firstPoint.get(1).doubleValue() != lastPoint.get(1).doubleValue()
        || //
        (firstPoint.size() == 3
            && firstPoint.get(2).doubleValue() != lastPoint.get(2).doubleValue())) {
      throw new InvalidGeometryException(
          "Expected first and last point to be the same in the LinearRing coordinates");
    }
  }

  /**
   * A helper method that validates the given object as GeoJSON Polygon coordinates.
   *
   * @param raw the object to test.
   * @throws InvalidGeometryException if the given object does not reflect Polygon coordinates.
   */
  public static void validatePolygonCoordinates(final Object raw) throws InvalidGeometryException {
    if (!(raw instanceof List)) {
      throw new InvalidGeometryException("Coordinates are expected to be an array");
    }
    @SuppressWarnings("unchecked")
    final List<Object> linearRings = (List<Object>) raw;
    if (linearRings.size() == 0) {
      throw new InvalidGeometryException("Polygon must have at least one LinearRing");
    }
    for (int i = 0; i < linearRings.size(); i++) {
      final Object linearRing = linearRings.get(i);
      if (!(linearRing instanceof List)) {
        throw new InvalidGeometryException(
            "Expected LinearRing at index #" + i + " of the Polygon coordinates");
      }
      validateLinearRingCoordinates(linearRing, i == 0);
    }
  }

  /**
   * A helper method that validates the given object as GeoJSON MultiPolygon coordinates.
   *
   * @param raw the object to test.
   * @throws InvalidGeometryException if the given object does not reflect MultiPolygon coordinates.
   */
  public static void validateMultiPolygonCoordinates(final Object raw) throws InvalidGeometryException {
    if (!(raw instanceof List)) {
      throw new InvalidGeometryException("Coordinates are expected to be an array");
    }
    @SuppressWarnings("unchecked")
    final List<Object> polygons = (List<Object>) raw;
    if (polygons.size() == 0) {
      throw new InvalidGeometryException("MultiPolygon must have at least one Polygon");
    }
    for (int i = 0; i < polygons.size(); i++) {
      final Object polygon = polygons.get(i);
      if (!(polygon instanceof List)) {
        throw new InvalidGeometryException(
            "Expected Polygon at index #" + i + " of the MultiPolygon coordinates");
      }
      validatePolygonCoordinates(polygon);
    }
  }

  @JsonGetter
  public BBox getBBox() {
    return bbox;
  }

  @JsonSetter
  public void setBBox(BBox bbox) {
    this.bbox = bbox;
  }

  public XyzGeometry withBBox(final BBox bbox) {
    setBBox(bbox);
    return this;
  }

  @JsonIgnore
  @SuppressWarnings("WeakerAccess")
  public org.locationtech.jts.geom.Geometry getJTSGeometry() {
    if (geomCache == null) {
      geomCache = convertToJTSGeometry();
    }

    return geomCache;
  }

  // TODO: Please fix the "isExterior" parameter that is currently unused (either use it or remove
  // it)

  protected abstract org.locationtech.jts.geom.Geometry convertToJTSGeometry();

  public abstract BBox calculateBBox();

  /**
   * Validates the geometry.
   *
   * @throws InvalidGeometryException if the geometry is invalid.
   */
  public abstract void validate() throws InvalidGeometryException;
}
