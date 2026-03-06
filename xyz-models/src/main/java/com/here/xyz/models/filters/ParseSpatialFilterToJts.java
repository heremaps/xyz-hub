package com.here.xyz.models.filters;

@FunctionalInterface
public interface ParseSpatialFilterToJts {
  /**
   * Parses the given spatial filter to a JTS geometry applying the necessary buffer.
   *
   * @param spatialFilter the spatial filter to parse
   * @return the parsed JTS geometry with the applied buffer if specified in the spatial filter
   * @throws IllegalArgumentException if the spatial filter is invalid
   */
  org.locationtech.jts.geom.Geometry parseAndApplyBuffer(SpatialFilter spatialFilter) throws IllegalArgumentException;
}
