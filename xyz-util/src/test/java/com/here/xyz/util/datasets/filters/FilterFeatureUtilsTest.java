/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */

package com.here.xyz.util.datasets.filters;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.filters.Filters;
import com.here.xyz.models.filters.ParseSpatialFilterToJts;
import com.here.xyz.models.filters.SpatialFilter;
import com.here.xyz.models.geojson.coordinates.LinearRingCoordinates;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.coordinates.PolygonCoordinates;
import com.here.xyz.models.geojson.coordinates.Position;
import com.here.xyz.models.geojson.exceptions.InvalidGeometryException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Polygon;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;

class FilterFeatureUtilsTest {

  private static final GeometryFactory GEOMETRY_FACTORY = new GeometryFactory();
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String JSON_PATH_FOR_UNCHANGED_ID = "$[?(@.id == 'Q45671')]";

  private String featureJsonString;
  private Feature featureWithRightId;

  @BeforeEach
  void setUp() throws IOException, URISyntaxException {
    featureJsonString = Files.readString(Paths.get(
        FilterFeatureUtilsTest.class.getResource("/test/feature_example.json").toURI()));
    featureWithRightId = MAPPER.readValue(featureJsonString, Feature.class);
  }

  @Test
  void filterFeaturesNullFilters() {
    Collection<Feature> filteredFeatures = FilterFeatureUtils.filterFeatures(List.of(Pair.of(featureWithRightId, new Point())),
        null);
    assertFalse(filteredFeatures.isEmpty());
  }

  @Test
  void filterFeaturesEmptyFilters() {
    Collection<Feature> filteredFeatures = FilterFeatureUtils.filterFeatures(List.of(Pair.of(featureWithRightId, new Point())),
        new Filters());
    assertFalse(filteredFeatures.isEmpty());
  }


  @Test
  void filterFeaturesInvalidJsonPath() {
    Filters filters = new Filters().withJsonPath("$[?(@.id == 'empty')]");
    Collection<Feature> filteredFeatures = FilterFeatureUtils.filterFeatures(List.of(Pair.of(featureWithRightId, new Point())),
        filters);
    assertEquals(0, filteredFeatures.size());
  }

  @Test
  void filterFeaturesNullSpatialFilter() throws JsonProcessingException {
    Feature featureWithChangedId = MAPPER.readValue(featureJsonString, Feature.class);
    featureWithChangedId.setId("TEST_ID");
    List<Pair<Feature, Geometry>> featuresWithGeometries = List.of(
        Pair.of(featureWithRightId, new Point()),
        Pair.of(featureWithChangedId, new Point()));
    Collection<Feature> filteredFeatures = FilterFeatureUtils.filterFeatures(featuresWithGeometries,
        new Filters().withJsonPath(JSON_PATH_FOR_UNCHANGED_ID));
    assertEquals(1, filteredFeatures.size());
    assertSame(featureWithRightId, filteredFeatures.iterator().next());
  }

  @Test
  void filterFeaturesIntersecting() throws InvalidGeometryException {
    Point intersectingGeometryPoint = new Point().withCoordinates(new PointCoordinates(1.0, 1.0)); // inside the square from (0,0) to (2,2)
    SpatialFilter spatialFilter = getTestSpatialFilter();

    List<Pair<Feature, Geometry>> featuresWithGeometries = List.of(Pair.of(featureWithRightId, intersectingGeometryPoint));
    Collection<Feature> filteredFeatures = FilterFeatureUtils.filterFeatures(featuresWithGeometries,
        new Filters().withJsonPath(JSON_PATH_FOR_UNCHANGED_ID).withSpatialFilter(spatialFilter));
    assertEquals(1, filteredFeatures.size());
    assertSame(featureWithRightId, filteredFeatures.iterator().next());
  }

  @Test
  void filterFeaturesOnePassingOneNotIntersectingAndOneWrongJsonPath() throws InvalidGeometryException, JsonProcessingException {
    Feature featureWithChangedId = MAPPER.readValue(featureJsonString, Feature.class);
    featureWithChangedId.setId("TEST_ID");

    Point intersectingGeometryPoint = new Point().withCoordinates(new PointCoordinates(1.0, 1.0));
    Point notIntersectingGeometryPoint = new Point().withCoordinates(
        new PointCoordinates(3.0, 3.0)); // outside the square from (0,0) to (2,2)
    SpatialFilter spatialFilter = getTestSpatialFilter();

    List<Pair<Feature, Geometry>> featuresWithGeometries = List.of(
        Pair.of(featureWithChangedId, intersectingGeometryPoint),
        Pair.of(featureWithRightId, intersectingGeometryPoint),
        Pair.of(featureWithRightId, notIntersectingGeometryPoint));
    Collection<Feature> filteredFeatures = FilterFeatureUtils.filterFeatures(featuresWithGeometries,
        new Filters().withJsonPath(JSON_PATH_FOR_UNCHANGED_ID).withSpatialFilter(spatialFilter));
    assertEquals(1, filteredFeatures.size());
    assertSame(featureWithRightId, filteredFeatures.iterator().next());
  }

  @Test
  void filterFeaturesOneRadiusIntersectingAndOneRadiusWrong() throws InvalidGeometryException {
    Point insideBufferPoint = new Point().withCoordinates(new PointCoordinates(2.01, 2.01));
    Point outsideBufferPoint = new Point().withCoordinates(new PointCoordinates(3.0, 3.0));
    SpatialFilter spatialFilter = getTestSpatialFilter().withRadius(5500);

    List<Pair<Feature, Geometry>> featuresWithGeometries = List.of(Pair.of(featureWithRightId, insideBufferPoint),
        Pair.of(featureWithRightId, outsideBufferPoint));
    Collection<Feature> filteredFeatures = FilterFeatureUtils.filterFeatures(featuresWithGeometries,
        new Filters().withJsonPath(JSON_PATH_FOR_UNCHANGED_ID).withSpatialFilter(spatialFilter));
    assertEquals(1, filteredFeatures.size());
  }

  @Test
  public void anyFeatureMatchesFiltersIntersecting() throws InvalidGeometryException {
    Point intersectingGeometryPoint = new Point().withCoordinates(new PointCoordinates(1.0, 1.0)); // inside the square from (0,0) to (2,2)
    SpatialFilter spatialFilter = getTestSpatialFilter();

    List<Pair<Feature, Geometry>> featuresWithGeometries = List.of(Pair.of(featureWithRightId, intersectingGeometryPoint));
    boolean containsFeature = FilterFeatureUtils.anyFeatureMatchesFilters(featuresWithGeometries,
        new Filters().withJsonPath(JSON_PATH_FOR_UNCHANGED_ID).withSpatialFilter(spatialFilter));
    assertTrue(containsFeature);
  }

  @Test
  public void anyFeatureMatchesFiltersNotIntersecting() throws InvalidGeometryException {
    Point intersectingGeometryPoint = new Point().withCoordinates(new PointCoordinates(3.0, 3.0)); // outside the square from (0,0) to (2,2)
    SpatialFilter spatialFilter = getTestSpatialFilter();

    List<Pair<Feature, Geometry>> featuresWithGeometries = List.of(Pair.of(featureWithRightId, intersectingGeometryPoint));
    boolean containsFeature = FilterFeatureUtils.anyFeatureMatchesFilters(featuresWithGeometries,
        new Filters().withJsonPath(JSON_PATH_FOR_UNCHANGED_ID).withSpatialFilter(spatialFilter));
    assertFalse(containsFeature);
  }

  @Test
  public void isFeatureMatchingFiltersIntersecting() throws InvalidGeometryException {
    Point intersectingGeometryPoint = new Point().withCoordinates(new PointCoordinates(1.0, 1.0)); // inside the square from (0,0) to (2,2)
    SpatialFilter spatialFilter = getTestSpatialFilter();
    boolean matchesFilters = FilterFeatureUtils.isFeatureMatchingFilters(featureWithRightId, intersectingGeometryPoint,
        new Filters().withJsonPath(JSON_PATH_FOR_UNCHANGED_ID).withSpatialFilter(spatialFilter), getParseSpatialFilterToJts());
    assertTrue(matchesFilters);
  }

  @Test
  public void isFeatureMatchingFiltersNotIntersecting() throws InvalidGeometryException {
    Point intersectingGeometryPoint = new Point().withCoordinates(new PointCoordinates(3.0, 3.0)); // inside the square from (0,0) to (2,2)
    SpatialFilter spatialFilter = getTestSpatialFilter();
    boolean matchesFilters = FilterFeatureUtils.isFeatureMatchingFilters(featureWithRightId, intersectingGeometryPoint,
        new Filters().withJsonPath(JSON_PATH_FOR_UNCHANGED_ID).withSpatialFilter(spatialFilter), getParseSpatialFilterToJts());
    assertFalse(matchesFilters);
  }

  // Returns a spatial filter of a square polygon from (0,0) to (2,2)
  private SpatialFilter getTestSpatialFilter() throws InvalidGeometryException {
    PolygonCoordinates polygonCoordinates = new PolygonCoordinates();
    LinearRingCoordinates lrc = new LinearRingCoordinates();
    lrc.addAll(List.of(
        new Position(0.0, 0.0),
        new Position(0.0, 2.0),
        new Position(2.0, 2.0),
        new Position(2.0, 0.0),
        new Position(0.0, 0.0)));
    polygonCoordinates.add(lrc);
    return new SpatialFilter().withGeometry(new Polygon().withCoordinates(polygonCoordinates));
  }

  // Returns a ParseSpatialFilterToJts implementation that parses the spatial filter to a square polygon from (0,0) to (2,2) regardless of the actual spatial filter geometry
  private ParseSpatialFilterToJts getParseSpatialFilterToJts() {
    return spatialFilter -> {
      try {
        return GEOMETRY_FACTORY.createPolygon(new Coordinate[]{
            new Coordinate(0.0, 0.0),
            new Coordinate(0.0, 2.0),
            new Coordinate(2.0, 2.0),
            new Coordinate(2.0, 0.0),
            new Coordinate(0.0, 0.0)});
      } catch (Exception e) {
        throw new IllegalArgumentException("Error applying buffer to spatial filter geometry", e);
      }
    };
  }
}