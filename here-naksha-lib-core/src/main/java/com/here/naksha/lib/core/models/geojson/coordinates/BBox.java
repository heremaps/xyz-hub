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
package com.here.naksha.lib.core.models.geojson.coordinates;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.ArrayList;

/**
 * A bounding box that follows the GeoJson specification, which states that: The value MUST be an
 * array of length 2*n where n is the number of dimensions represented in the contained geometries,
 * with all axes of the most southwesterly point followed by all axes of the more northeasterly
 * point. The axes order of a bbox follows the axes order of geometries.
 *
 * @see <a href="https://tools.ietf.org/html/rfc7946#section-5">RFC-7946 Section-5</a>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class BBox extends ArrayList<Double> {

  private static final int MIN_LON = 0;
  private static final int MIN_LAT = 1;
  private static final int MAX_LON = 2;
  private static final int MAX_LAT = 3;

  /** Generate an empty bounding box with all bounds set to zero (west, south, east and north). */
  public BBox() {
    super();
  }

  /**
   * Create a new bounding box with the given predefined values.
   *
   * @param west the west longitude (minimal longitude).
   * @param south the south latitude (minimal latitude).
   * @param east the east longitude (maximal longitude).
   * @param north the north latitude (maximal latitude).
   */
  public BBox(double west, double south, double east, double north) {
    setWest(west);
    setSouth(south);
    setEast(east);
    setNorth(north);
  }

  private void ensureSize() {
    for (int i = size(); i < 4; i++) {
      add(null);
    }
  }

  /**
   * Return the west longitude.
   *
   * @return the west longitude
   */
  public double getWest() {
    return get(MIN_LON);
  }

  @SuppressWarnings("WeakerAccess")
  public void setWest(double west) {
    ensureSize();
    set(MIN_LON, west);
  }

  /**
   * Sets the west longitude (minimal longitude).
   *
   * @param west the west longitude.
   * @return this.
   */
  @SuppressWarnings("unused")
  public BBox withWest(double west) {
    setWest(west);
    return this;
  }

  /**
   * Return the south latitude.
   *
   * @return the south latitude.
   */
  public double getSouth() {
    return get(MIN_LAT);
  }

  @SuppressWarnings("WeakerAccess")
  public void setSouth(double south) {
    ensureSize();
    set(MIN_LAT, south);
  }

  /**
   * Sets the south latitude (minimal latitude).
   *
   * @param south the south latitude.
   * @return this.
   */
  @SuppressWarnings("unused")
  public BBox withSouth(double south) {
    setSouth(south);
    return this;
  }

  /**
   * Return the east longitude.
   *
   * @return the east longitude
   */
  public double getEast() {
    return get(MAX_LON);
  }

  @SuppressWarnings("WeakerAccess")
  public void setEast(double east) {
    ensureSize();
    set(MAX_LON, east);
  }

  /**
   * Sets the east longitude (maximal longitude).
   *
   * @param east the east longitude.
   * @return this.
   */
  @SuppressWarnings("unused")
  public BBox withEast(double east) {
    setEast(east);
    return this;
  }

  /**
   * Return the north latitude.
   *
   * @return the north latitude.
   */
  public double getNorth() {
    return get(MAX_LAT);
  }

  @SuppressWarnings("WeakerAccess")
  public void setNorth(double north) {
    ensureSize();
    set(MAX_LAT, north);
  }

  /**
   * Sets the north latitude (maximal latitude).
   *
   * @param north the north latitude.
   * @return this.
   */
  @SuppressWarnings("unused")
  public BBox withNorth(double north) {
    setNorth(north);
    return this;
  }

  /**
   * Return the west longitude.
   *
   * @return the west longitude
   */
  public double minLon() {
    return getWest();
  }

  /**
   * Return the south latitude.
   *
   * @return the south latitude.
   */
  public double minLat() {
    return getSouth();
  }

  /**
   * Return the east longitude.
   *
   * @return the east longitude
   */
  public double maxLon() {
    return getEast();
  }

  /**
   * Return the north latitude.
   *
   * @return the north latitude.
   */
  public double maxLat() {
    return getNorth();
  }

  /**
   * Returns the longitude distance in degree.
   *
   * @param shortestDistance If true, then the shortest distance is returned, that means when
   *     crossing the date border is shorter than the other way around, this is returned. When
   *     false, then the date border is never crossed, what will result in bigger bounding boxes.
   * @return the longitude distance in degree.
   */
  @SuppressWarnings("unused")
  public double widthInDegree(boolean shortestDistance) {
    if (shortestDistance) {
      // Note: Because the earth is a sphere there are two directions into which we can move, for
      // example:
      // min: -170° max: +170°
      // The distance here can be either 340° (heading west) or only 20° (heading east and crossing
      // the date border).
      final double direct = Math.abs(maxLon() - minLon()); // +170 - -170 = +340
      final double crossDateBorder = 360 - direct; // 360 - 340 = 20
      // In the above example crossing the date border is the shorted distance and therefore we take
      // it as requested.
      return Math.min(direct, crossDateBorder);
    }
    return (maxLon() + 180d) - (minLon() + 180d);
  }

  /**
   * Returns the shortest latitude distance in degree.
   *
   * @return the shortest latitude distance in degree.
   */
  @SuppressWarnings("unused")
  public double heightInDegree() {
    final double max = maxLat();
    final double min = minLat();

    // Should be:
    if (min < max) {
      return (max + 90d) - (min + 90d);
    }

    // Must not be, but lets fix it anyway:
    return (min + 90d) - (max + 90d);
  }
}
