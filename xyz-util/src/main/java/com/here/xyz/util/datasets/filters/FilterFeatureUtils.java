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

import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.util.geo.GeoTools;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.jayway.jsonpath.JsonPath;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Predicate;
import javax.xml.crypto.dsig.TransformException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.geotools.api.referencing.FactoryException;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;

public class FilterFeatureUtils {

  private static final Logger logger = LogManager.getLogger();
  private static final PreparedGeometryFactory GEOMETRY_FACTORY = new PreparedGeometryFactory();

  private FilterFeatureUtils() {
  }

  public static Collection<Feature> filterFeatures(Collection<Pair<Feature, Geometry>> featuresWithGeometries, Collection<String> jsonPaths,
      SpatialFilter spatialFilter) {
    if (featuresWithGeometries == null) {
      return Collections.emptyList();
    }
    try {
      Predicate<Pair<Feature, Geometry>> filteringPredicate = getFilteringPredicate(jsonPaths, spatialFilter);
      return featuresWithGeometries.stream()
          .filter(filteringPredicate)
          .map(Pair::getLeft)
          .toList();
    } catch (ValidationException e) {
      logger.warn("Spatial filter validation failed when filtering features: {}", e.getMessage());
      return Collections.emptyList();
    }
  }

  public static boolean anyFeatureMatchesFilters(Collection<Pair<Feature, Geometry>> featuresWithGeometries, Collection<String> jsonPaths,
      SpatialFilter spatialFilter) {
    if (featuresWithGeometries == null) {
      return false;
    }
    try {
      Predicate<Pair<Feature, Geometry>> filteringPredicate = getFilteringPredicate(jsonPaths, spatialFilter);
      return featuresWithGeometries.stream()
          .anyMatch(filteringPredicate);
    } catch (ValidationException e) {
      logger.warn("Spatial filter validation failed when verifying feature: {}", e.getMessage());
      return false;
    }
  }

  // Creates a features filtering predicate based on the provided JSON paths and spatial filter.
  private static Predicate<Pair<Feature, Geometry>> getFilteringPredicate(Collection<String> jsonPaths, SpatialFilter spatialFilter)
      throws ValidationException {
    Collection<JsonPath> compiledJsonPaths = jsonPaths == null ? Collections.emptyList() : jsonPaths.stream()
        .filter(Objects::nonNull)
        .map(JsonPath::compile)
        .toList();
    // If no spatial filter is provided, only filter by JSON paths.
    if (spatialFilter == null || spatialFilter.getGeometry() == null || spatialFilter.getGeometry().getJTSGeometry() == null) {
      logger.warn("Provided null spatial filter when filtering features");
      return featureGeometryPair -> featureGeometryPair != null && JsonPathFilterUtils.filterByJsonPaths(featureGeometryPair.getLeft(),
          compiledJsonPaths);
    }
    // Else, create prepared geometry from spatial filter and filter by both JSON paths and spatial filter geometry.
    PreparedGeometry preparedGeometry = getPreparedGeometryFromSpatialFilter(spatialFilter);
    return featureGeometryPair -> filterFeatureWithJsonPathsAndGeometry(featureGeometryPair, compiledJsonPaths, preparedGeometry);
  }

  private static boolean filterFeatureWithJsonPathsAndGeometry(Pair<Feature, Geometry> featureWithGeometry,
      Collection<JsonPath> jsonPaths,
      PreparedGeometry preparedGeometry) {
    if (featureWithGeometry == null) {
      logger.warn("Provided null feature and geometry pair when filtering feature");
      return false;
    }
    return JsonPathFilterUtils.filterByJsonPaths(featureWithGeometry.getLeft(), jsonPaths)
        && isGeometryIntersectingFilterPreparedGeometry(featureWithGeometry.getRight(), preparedGeometry);
  }

  private static boolean isGeometryIntersectingFilterPreparedGeometry(Geometry featureGeometry, PreparedGeometry preparedGeometry) {
    if (featureGeometry == null || featureGeometry.getJTSGeometry() == null || preparedGeometry == null) {
      return false;
    }
    org.locationtech.jts.geom.Geometry jtsGeometry = featureGeometry.getJTSGeometry();
    return jtsGeometry.isValid() && preparedGeometry.intersects(jtsGeometry);
  }

  private static PreparedGeometry getPreparedGeometryFromSpatialFilter(SpatialFilter spatialFilter) throws ValidationException {
    spatialFilter.validateSpatialFilter();
    try {
      org.locationtech.jts.geom.Geometry bufferedGeometry = GeoTools.applyBufferInMetersToGeometry(
          spatialFilter.getGeometry().getJTSGeometry(), spatialFilter.getRadius());
      return GEOMETRY_FACTORY.create(bufferedGeometry);
    } catch (FactoryException | TransformException | org.geotools.api.referencing.operation.TransformException e) {
      logger.error("Encountered error when applying buffering in spatial filter: {}", e.getMessage());
      throw new ValidationException("Error applying buffer to spatial filter geometry", e);
    }
  }
}
