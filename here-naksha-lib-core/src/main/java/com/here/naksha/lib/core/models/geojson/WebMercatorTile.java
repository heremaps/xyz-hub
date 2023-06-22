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

package com.here.naksha.lib.core.models.geojson;

import com.here.naksha.lib.core.models.geojson.coordinates.BBox;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.declaration.ILonLat;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.prep.PreparedGeometry;
import com.vividsolutions.jts.geom.prep.PreparedGeometryFactory;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class that represents a Web Mercator Tile with additional helper methods to perform
 * calculations with Web Mercator projection.
 */
@SuppressWarnings("WeakerAccess")
public class WebMercatorTile {

  /** Earth radius in meter. */
  public static final double EarthRadius = 6378137;

  /** Minimal latitude for Web Mercator projection. */
  public static final double MinLatitude = -85.05112878;

  /** Maximal latitude for Web Mercator projection. */
  public static final double MaxLatitude = 85.05112878;

  /** Minimal longitude for Web Mercator projection. */
  public static final double MinLongitude = -180;

  /** Maximal longitude for Web Mercator projection. */
  public static final double MaxLongitude = 180;

  public static final int TileSizeInPixel = 256;

  private static final Pattern QUADKEY_REGEXP = Pattern.compile("[0-3]*");
  private static final Pattern QUADPIXEL_REGEXP = Pattern.compile("([0-9]*)_([0-9]*)_([0-9]*)");
  private static final int MAX_LEVEL = 20; // Why 20 and not 23?
  private static final double WORLD_IN_METER = 40075016.685578488d;
  private static final double HALF_WORLD_IN_METER = 20037508.342789244d;
  /** The pixel X-coordinate in the level of detail. */
  public final int x;
  /** The pixel y-coordinate in the level of detail. */
  public final int y;
  /** The level of detail. */
  public final int level;
  /** The Mercator meters of the left tile border. */
  public final double left;
  /** The Mercator meters of the top tile border. */
  public final double top;
  /** The Mercator meters of the right tile border. */
  public final double right;
  /** The Mercator meters of the bottom tile border. */
  public final double bottom;

  private BBox bbox;
  private PreparedGeometry polygon;
  private String quadkey;
  private BBox eBbox;
  private int eBuffer;
  private PreparedGeometry ePolygon;
  private int ePolygonBuffer;

  private WebMercatorTile(final int level, final int x, final int y) {
    this.level = level;
    this.x = x;
    this.y = y;

    final double worldInPixel = 1L << level;
    this.left = ((((double) x) / worldInPixel) * WORLD_IN_METER) - HALF_WORLD_IN_METER;
    this.right = ((((double) (x + 1)) / worldInPixel) * WORLD_IN_METER) - HALF_WORLD_IN_METER;
    this.bottom = ((((double) y) / worldInPixel) * WORLD_IN_METER) * -1 + HALF_WORLD_IN_METER;
    this.top = ((((double) (y + 1)) / worldInPixel) * WORLD_IN_METER) * -1 + HALF_WORLD_IN_METER;
  }

  /**
   * Merge the 32-bit x- and y-coordinates of pixel into one compact 64-bit pixel value.
   *
   * @param x The Web Mercator x-coordinate of the pixel.
   * @param y The Web Mercator y-coordinate of the pixel.
   * @return The compact 64-bit pixel.
   */
  public static long xy(int x, int y) {
    return (Integer.toUnsignedLong(y) << 32) | Integer.toUnsignedLong(x);
  }

  /**
   * Returns the x-coordinate of a 64-bit compact pixel.
   *
   * @param pixel The compact 64-bit pixel.
   * @return the x-coordinate of the pixel.
   */
  public static int x(long pixel) {
    return (int) (pixel & 0xffff_ffffL);
  }

  /**
   * Returns the y-coordinate of a 64-bit compact pixel.
   *
   * @param pixel The compact 64-bit pixel.
   * @return the y-coordinate of the pixel.
   */
  public static int y(long pixel) {
    return (int) (pixel >>> 32);
  }

  /**
   * Clips a number to the specified minimum and maximum values.
   *
   * @param n The number to clip.
   * @param minValue Minimum allowable value.
   * @param maxValue Maximum allowable value.
   * @return The clipped value.
   */
  public static double clip(double n, double minValue, double maxValue) {
    return Math.min(Math.max(n, minValue), maxValue);
  }

  /**
   * Verify the level of detail and returns it.
   *
   * @param levelOfDetail the level of detail to verify.
   * @return The level of detail if it is a value between 1 (lowest detail) and (including) 23
   *     (highest detail).
   * @throws IllegalArgumentException if the given levelOfDetail is less than 1 or more than 23.
   */
  public static int verifyLevelOfDetail(int levelOfDetail) throws IllegalArgumentException {
    if (levelOfDetail < 1) {
      throw new IllegalArgumentException("levelOfDetail is less than 1");
    }
    if (levelOfDetail > 23) {
      throw new IllegalArgumentException("levelOfDetail is more than 23");
    }
    return levelOfDetail;
  }

  /**
   * Determines the map width and height (in pixels) at a specified level of detail.
   *
   * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
   * @return The map width and height in pixels.
   */
  public static long mapSize(int levelOfDetail) throws IllegalArgumentException {
    return 256L << levelOfDetail;
  }

  /**
   * Determines the ground resolution (in meters per pixel) at a specified latitude and level of
   * detail.
   *
   * @param latitude Latitude (in degrees) at which to measure the ground resolution.
   * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
   * @return The ground resolution, in meters per pixel.
   */
  public static double groundResolution(double latitude, int levelOfDetail) throws IllegalArgumentException {
    latitude = clip(latitude, MinLatitude, MaxLatitude);
    return Math.cos(latitude * Math.PI / 180) * 2 * Math.PI * EarthRadius / mapSize(levelOfDetail);
  }

  /**
   * Determines the map scale at a specified latitude, level of detail, and screen resolution.
   *
   * @param latitude Latitude (in degrees) at which to measure the map scale.
   * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
   * @param screenDpi Resolution of the screen, in dots per inch.
   * @return The map scale, expressed as the denominator N of the ratio 1 : N.
   */
  public static double mapScale(double latitude, int levelOfDetail, int screenDpi) {
    return groundResolution(latitude, levelOfDetail) * screenDpi / 0.0254d;
  }

  /**
   * Converts a point from latitude/longitude WGS-84 coordinates (in degrees) into pixel XY
   * coordinates at a specified level of detail.
   *
   * @param longitude Longitude of the point, in degrees.
   * @param latitude Latitude of the point, in degrees.
   * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
   * @return The pixel (x- and y-coordinate in 64-bit compact form).
   */
  public static long lonLatToPixel(double longitude, double latitude, int levelOfDetail) {
    latitude = clip(latitude, MinLatitude, MaxLatitude);
    longitude = clip(longitude, MinLongitude, MaxLongitude);

    double x = (longitude + 180) / 360;
    double sinLatitude = Math.sin(latitude * Math.PI / 180);
    double y = 0.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI);

    long mapSize = mapSize(levelOfDetail);
    int pixelX = (int) clip(x * mapSize + 0.5, 0, mapSize - 1);
    int pixelY = (int) clip(y * mapSize + 0.5, 0, mapSize - 1);
    return xy(pixelX, pixelY);
  }

  /**
   * Converts a pixel from pixel XY coordinates at a specified level of detail into
   * latitude/longitude WGS-84 coordinates (in degrees).
   *
   * @param pixelX X coordinate of the point, in pixels.
   * @param pixelY Y coordinates of the point, in pixels.
   * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
   * @param pos If given, then the latitude and longitude values are stored in that object and the
   *     object is returned, if null, a new position instance is created.
   * @return the position with the longitude and latitude value.
   */
  public static ILonLat pixelToLonLat(int pixelX, int pixelY, int levelOfDetail, ILonLat pos) {
    double mapSize = mapSize(levelOfDetail);
    double x = (clip(pixelX, 0, mapSize - 1) / mapSize) - 0.5;
    double y = 0.5 - (clip(pixelY, 0, mapSize - 1) / mapSize);

    double latitude = 90 - 360 * Math.atan(Math.exp(-y * 2 * Math.PI)) / Math.PI;
    double longitude = 360 * x;
    if (pos == null) {
      return new LonLat(longitude, latitude);
    }
    pos.setLatitude(latitude);
    pos.setLongitude(longitude);
    return pos;
  }

  /**
   * Converts pixel XY coordinates into tile XY coordinates of the tile containing the specified
   * pixel.
   *
   * @param pixelX Pixel X coordinate.
   * @param pixelY Pixel Y coordinate.
   * @return The X and Y coordinate of the tile in which the pixel is located.
   */
  public static long pixelToTile(int pixelX, int pixelY) {
    int tileX = pixelX / 256;
    int tileY = pixelY / 256;
    return xy(tileX, tileY);
  }

  /**
   * Converts tile XY coordinates into pixel XY coordinates of the upper-left pixel of the specified
   * tile.
   *
   * @param tileX Tile X coordinate.
   * @param tileY Tile Y coordinate.
   * @return The X and Y coordinate of the pixel of the upper-left pixel of the specified tile.
   */
  public static long tileToPixel(int tileX, int tileY) {
    int pixelX = tileX * 256;
    int pixelY = tileY * 256;
    return xy(pixelX, pixelY);
  }

  /**
   * Converts tile XY coordinates into a QuadKey at a specified level of detail.
   *
   * @param tileX Tile X coordinate.
   * @param tileY Tile Y coordinate.
   * @param levelOfDetail Level of detail, from 1 (lowest detail) to 23 (highest detail).
   * @return A string containing the QuadKey.
   */
  public static String tileToQuadKey(int tileX, int tileY, int levelOfDetail) {
    StringBuilder quadKey = new StringBuilder();
    for (int i = levelOfDetail; i > 0; i--) {
      char digit = '0';
      int mask = 1 << (i - 1);
      if ((tileX & mask) != 0) {
        digit++;
      }
      if ((tileY & mask) != 0) {
        digit++;
        digit++;
      }
      quadKey.append(digit);
    }
    return quadKey.toString();
  }

  /**
   * Returns the level of detail of the given quadKey.
   *
   * @param quadkey The quadKey.
   * @return The level of detail of that quadKey.
   */
  public static int quadKeyLevelOfDetail(String quadkey) {
    if (quadkey == null) {
      return 0;
    }
    return quadkey.length();
  }

  /**
   * Returns the level of detail of a given BBOX (which represents a Tile)
   *
   * @param bbox of tile
   * @return ZoomLevel of tile
   */
  public static int getZoomFromBBOX(BBox bbox) {
    return (int) Math.ceil(Math.log((360d / bbox.widthInDegree(false))) / Math.log(2));
  }

  /**
   * Returns Tile for are Latitude,Longitude Coordinate for a given Level
   *
   * @param lat Latitude of the point, in degrees.
   * @param lon Longitude of the point, in degrees.
   * @param lev levelOfDetail Level of detail
   * @return
   */
  public static WebMercatorTile getTileFromLatLonLev(double lat, double lon, int lev) {
    double sinLatitude = Math.sin(lat * Math.PI / 180),
        row = Math.floor(((lon + 180) / 360) * Math.pow(2, lev)),
        col =
            Math.floor((.5 - Math.log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * Math.PI))
                * Math.pow(2, lev));

    return WebMercatorTile.forWeb(lev, (int) row, (int) col);
  }

  /**
   * Converts a QuadKey into tile XY coordinates.
   *
   * @param quadKey QuadKey of the tile.
   * @return The tile x and y coordinate.
   * @throws NullPointerException If the given quadKey argument is null.
   * @throws IllegalArgumentException If the given quadKey argument contains an invalid digit or is
   *     too short/long.
   */
  public static long quadKeyToTile(String quadKey) throws NullPointerException, IllegalArgumentException {
    if (quadKey == null) {
      throw new NullPointerException("quadkey argument is null");
    }
    int tileX = 0;
    int tileY = 0;
    final int levelOfDetail = quadKey.length();
    if (levelOfDetail == 0 || levelOfDetail > 23) {
      throw new IllegalArgumentException("quadkey argument must be between 1 and 23 characters");
    }
    for (int i = levelOfDetail; i > 0; i--) {
      int mask = 1 << (i - 1);
      switch (quadKey.charAt(levelOfDetail - i)) {
        case '0':
          break;

        case '1':
          tileX |= mask;
          break;

        case '2':
          tileY |= mask;
          break;

        case '3':
          tileX |= mask;
          tileY |= mask;
          break;

        default:
          throw new IllegalArgumentException("Invalid QuadKey digit sequence at index " + (i - 1));
      }
    }
    return xy(tileX, tileY);
  }

  /**
   * Returns the Web Mercator Tile identified by the provided quadKey.
   *
   * @param quadKey the quaddKey that represents
   */
  public static WebMercatorTile forQuadkey(String quadKey) throws IllegalArgumentException {
    if (!QUADKEY_REGEXP.matcher(quadKey).matches()) {
      throw new IllegalArgumentException("Invalid quadkey.");
    }

    int level = quadKey.length();
    int x = 0;
    int y = 0;

    int depth = Math.min(MAX_LEVEL, quadKey.length());

    for (int index = 0; index < depth; ++index) {
      y <<= 1;
      x <<= 1;
      char nextChar = quadKey.charAt(index);
      if ('1' == nextChar) {
        x++;
      } else if ('2' == nextChar) {
        y++;
      } else if ('3' == nextChar) {
        x++;
        y++;
      }
    }

    return new WebMercatorTile(level, x, y);
  }

  /**
   * Returns the tile address for the Web Mercator level, x-axis and y-axis values.
   *
   * @param tileAddress The tile address provided as string in the format {level}_{x}_{y}.
   */
  public static WebMercatorTile forWeb(String tileAddress) throws IllegalArgumentException {
    final Matcher pixelMatcher = QUADPIXEL_REGEXP.matcher(tileAddress);

    if (!pixelMatcher.matches()) {
      throw new IllegalArgumentException("Invalid quadkey.");
    }

    int level = Integer.parseInt(pixelMatcher.group(1));
    int x = Integer.parseInt(pixelMatcher.group(2));
    int y = Integer.parseInt(pixelMatcher.group(3));

    return forWeb(level, x, y);
  }

  /** Returns the tile address for the Web Mercator level, x-axis and y-axis values. */
  public static WebMercatorTile forWeb(int level, int x, int y) throws IllegalArgumentException {
    if (level < 0) {
      throw new IllegalArgumentException("Invalid tile address.");
    }

    int tilesLength = 1 << level;
    if (x < 0 || x >= tilesLength || y < 0 || y >= tilesLength) {
      throw new IllegalArgumentException("Invalid tile address.");
    }

    return new WebMercatorTile(level, x, y);
  }

  /**
   * Returns the tile address for the OSGEO TMS level, x-axis and y-axis values.
   *
   * @param tileAddress The tile address provided as string in the format {level}_{x}_{y}.
   */
  public static WebMercatorTile forTMS(String tileAddress) throws IllegalArgumentException {
    final Matcher pixelMatcher = QUADPIXEL_REGEXP.matcher(tileAddress);

    if (!pixelMatcher.reset(tileAddress).matches()) {
      throw new IllegalArgumentException("Invalid tile address.");
    }

    int level = Integer.parseInt(pixelMatcher.group(1));
    int x = Integer.parseInt(pixelMatcher.group(2));
    int y = Integer.parseInt(pixelMatcher.group(3));

    return forTMS(level, x, y);
  }

  /** Returns the tile address for the OSGEO TMS level, x-axis and y-axis values. */
  public static WebMercatorTile forTMS(int level, int x, int y) throws IllegalArgumentException {
    if (level < 0) {
      throw new IllegalArgumentException("Invalid tile address.");
    }

    return forWeb(level, x, (1 << level) - 1);
  }

  public BBox getBBox(boolean clone) {
    if (bbox == null) {
      double mapSize = 1 << level;
      double x0 = (clip(x, 0, mapSize - 1) / mapSize) - 0.5;
      double y0 = 0.5 - (clip(y, 0, mapSize - 1) / mapSize);

      double maxLat = 90.0 - 360.0 * Math.atan(Math.exp(-y0 * 2.0 * Math.PI)) / Math.PI;
      double minLon = 360.0 * x0;

      double x1 = (clip(x + 1, 0, mapSize) / mapSize) - 0.5;
      double y1 = 0.5 - (clip(y + 1, 0, mapSize) / mapSize);

      double minLat = 90.0 - 360.0 * Math.atan(Math.exp(-y1 * 2.0 * Math.PI)) / Math.PI;
      double maxLon = 360.0 * x1;

      bbox = new BBox(minLon, minLat, maxLon, maxLat);
    }
    if (clone) {
      return new BBox(bbox.minLon(), bbox.minLat(), bbox.maxLon(), bbox.maxLat());
    }
    return bbox;
  }

  public PreparedGeometry getAsPolygon() {
    if (polygon != null) {
      return polygon;
    }

    final BBox bbox = getBBox(false);
    final Envelope envelope = new Envelope(bbox.minLon(), bbox.maxLon(), bbox.minLat(), bbox.maxLat());
    polygon = PreparedGeometryFactory.prepare(JTSHelper.factory.toGeometry(envelope));
    return polygon;
  }

  /**
   * Return
   *
   * @param buffer buffer size in pixels on the respective level.
   */
  public BBox getExtendedBBox(int buffer) {
    if (eBbox != null && eBuffer == buffer) {
      return bbox;
    }

    long TILE_SIZE_IN_PIXEL = TileSizeInPixel;
    double bufferRelative = (double) buffer / (double) TILE_SIZE_IN_PIXEL;

    double mapSize = 1 << level;
    double x0 = (clip(x - bufferRelative, 0, mapSize - 1) / mapSize) - 0.5;

    double y0 = 0.5 - (clip(y - bufferRelative, 0, mapSize - 1) / mapSize);

    double maxLat = 90.0 - 360.0 * Math.atan(Math.exp(-y0 * 2.0 * Math.PI)) / Math.PI;
    double minLon = 360.0 * x0;

    double x1 = (clip(x + bufferRelative + 1, 0, mapSize) / mapSize) - 0.5;

    double y1 = 0.5 - (clip(y + bufferRelative + 1, 0, mapSize) / mapSize);

    double minLat = 90.0 - 360.0 * Math.atan(Math.exp(-y1 * 2.0 * Math.PI)) / Math.PI;
    double maxLon = 360.0 * x1;

    eBuffer = buffer;
    eBbox = new BBox(minLon, minLat, maxLon, maxLat);
    return eBbox;
  }

  public PreparedGeometry getExtendedBBoxAsPolygon(int buffer) {
    if (ePolygon != null && ePolygonBuffer == buffer) {
      return ePolygon;
    }

    final BBox bbox = getExtendedBBox(buffer);
    final Envelope envelope = new Envelope(bbox.minLon(), bbox.maxLon(), bbox.minLat(), bbox.maxLat());

    ePolygonBuffer = buffer;
    ePolygon = PreparedGeometryFactory.prepare(JTSHelper.factory.toGeometry(envelope));
    return ePolygon;
  }

  /**
   * Returns the quadKey representation of this tile address.
   *
   * @return the quadKey representation of this tile address.
   */
  public String asQuadkey() {
    if (quadkey != null) {
      return quadkey;
    }

    char[] quads = new char[level];
    int level = this.level;
    int x = this.x;
    int y = this.y;

    for (int i = 1; i <= level; i++) {
      int l = 1 << (i - 1);
      if ((x & l) == 0 && (y & l) == 0) {
        quads[level - i] = '0';
      } else if ((x & l) == l && (y & l) == 0) {
        quads[level - i] = '1';
      } else if ((x & l) == 0 && (y & l) == l) {
        quads[level - i] = '2';
      } else {
        quads[level - i] = '3';
      }
    }

    quadkey = new String(quads);

    return quadkey;
  }

  @Override
  public String toString() {
    return this.level + "_" + this.x + "_" + this.y;
  }

  /**
   * Simple implementation of the {@link ILonLat} interface, limited to the Web Mercator projection.
   */
  public static class LonLat implements ILonLat {

    protected double lat;
    protected double lon;

    /** Create an empty latitude/longitude pair, located at the point 0,0. */
    public LonLat() {}
    /** Create an initialized latitude/longitude pair. */
    public LonLat(double longitude, double latitude) {
      setLongitude(longitude);
      setLatitude(latitude);
    }

    @Override
    public double latitude() {
      return lat;
    }

    @Override
    public void setLatitude(double lat) {
      this.lat = clip(lat, MinLatitude, MaxLatitude);
    }

    @Override
    public double longitude() {
      return lon;
    }

    @Override
    public void setLongitude(double lon) {
      this.lon = clip(lon, MinLongitude, MaxLongitude);
    }
  }
}
