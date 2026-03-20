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

import com.google.common.base.Predicates;
import com.here.xyz.filters.Filters;
import com.here.xyz.models.filters.ParseSpatialFilterToJts;
import com.here.xyz.models.filters.SpatialFilter;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Geometry;
import com.here.xyz.util.geo.GeoTools;
import com.here.xyz.util.geo.GeometryValidator;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.jayway.jsonpath.JsonPath;
import java.util.Collection;
import java.util.Collections;
import java.util.function.Predicate;
import javax.xml.crypto.dsig.TransformException;
import org.apache.commons.lang3.StringUtils;
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

  public static Collection<Feature> filterFeatures(Collection<Pair<Feature, Geometry>> featuresWithGeometries, Filters filters) {
    if (featuresWithGeometries == null) {
      return Collections.emptyList();
    }
    try {
      Predicate<Pair<Feature, Geometry>> filteringPredicate = getFilteringPredicate(filters);
      return featuresWithGeometries.stream()
          .filter(filteringPredicate)
          .map(Pair::getLeft)
          .toList();
    } catch (ValidationException e) {
      logger.warn("Spatial filter validation failed when filtering features: {}", e.getMessage());
      return Collections.emptyList();
    }
  }

  public static boolean anyFeatureMatchesFilters(Collection<Pair<Feature, Geometry>> featuresWithGeometries, Filters filters) {
    if (featuresWithGeometries == null) {
      return false;
    }
    try {
      Predicate<Pair<Feature, Geometry>> filteringPredicate = getFilteringPredicate(filters);
      return featuresWithGeometries.stream()
          .anyMatch(filteringPredicate);
    } catch (ValidationException e) {
      logger.warn("Spatial filter validation failed when verifying feature: {}", e.getMessage());
      return false;
    }
  }

  // Method to verify if a single feature matches the provided filters using a custom provided ParseSpatialFilterToJts implementation
  // for parsing the spatial filter and applying the buffer instead of the default GeoTools implementation.
  public static boolean isFeatureMatchingFilters(Feature feature, Geometry geometry, Filters filters, ParseSpatialFilterToJts parseSpatialFilterToJts) {
    if (feature == null || geometry == null) {
      return false;
    }
    try {
      Predicate<Pair<Feature, Geometry>> filteringPredicate = getFilteringPredicate(filters, parseSpatialFilterToJts);
      return filteringPredicate.test(Pair.of(feature, geometry));
    } catch (ValidationException e) {
      logger.warn("Spatial filter buffering using custom parsing failed when verifying feature: {}", e.getMessage());
      return false;
    }
  }

  private static Predicate<Pair<Feature, Geometry>> getFilteringPredicate(Filters filters) throws ValidationException {
    // By default, we parse the spatial filter using GeoTools to apply the necessary buffering to the spatial filter geometry.
    ParseSpatialFilterToJts parseSpatialFilterUsingGeoTools = spatialFilter -> {
      try {
        return GeoTools.applyBufferInMetersToGeometry(spatialFilter.getGeometry().getJTSGeometry(), spatialFilter.getRadius());
      } catch (FactoryException | TransformException | org.geotools.api.referencing.operation.TransformException e) {
        logger.error("Encountered error when applying buffering using geotools in spatial filter: {}", e.getMessage());
        throw new IllegalArgumentException("Error applying buffer to spatial filter geometry", e);
      }
    };
    return getFilteringPredicate(filters, parseSpatialFilterUsingGeoTools);
  }

  // Creates a features filtering predicate based on the provided Filters object with JSON path and spatial filter fields.
  private static Predicate<Pair<Feature, Geometry>> getFilteringPredicate(Filters filters, ParseSpatialFilterToJts parseSpatialFilter)
      throws ValidationException {
    // if no filters provided, return an always true predicate to filter out no features.
    if (filters == null) {
      return Predicates.alwaysTrue();
    }
    // If a JSON path is provided, compile it once and reuse the compiled JsonPath object for filtering all features.
    JsonPath compiledJsonPath = StringUtils.isEmpty(filters.getJsonPath()) ? null : JsonPath.compile(filters.getJsonPath());
    // If no spatial filter is provided, only filter by a given JSON path.
    SpatialFilter spatialFilter = filters.getSpatialFilter();
    if (spatialFilter == null || spatialFilter.getGeometry() == null || spatialFilter.getGeometry().getJTSGeometry() == null) {
      logger.warn("Provided null spatial filter when filtering features");
      return featureGeometryPair -> featureGeometryPair != null && JsonPathFilterUtils.filterByJsonPath(
          featureGeometryPair.getLeft(), compiledJsonPath);
    }
    // Else, create prepared geometry from spatial filter and filter by both a JSON path and spatial filter geometry.
    GeometryValidator.validateSpatialFilter(spatialFilter);
    try {
      // The provided spatial filter geometry is buffered using the provided ParseSpatialFilterToJts implementation
      org.locationtech.jts.geom.Geometry bufferedGeometry = parseSpatialFilter.parseAndApplyBuffer(spatialFilter);
      PreparedGeometry preparedGeometry = GEOMETRY_FACTORY.create(bufferedGeometry);
      return featureGeometryPair -> filterFeatureWithJsonPathAndGeometry(featureGeometryPair, compiledJsonPath, preparedGeometry);
    } catch (IllegalArgumentException e) {
      logger.error("Encountered an illegal argument when transforming a spatial filter to JTS geometry: {}", e.getMessage());
      throw new ValidationException("Error applying buffer to spatial filter geometry", e);
    }
  }

  private static boolean filterFeatureWithJsonPathAndGeometry(Pair<Feature, Geometry> featureWithGeometry,
      JsonPath jsonPath, PreparedGeometry preparedGeometry) {
    if (featureWithGeometry == null) {
      logger.warn("Provided null feature and geometry pair when filtering feature");
      return false;
    }
    return JsonPathFilterUtils.filterByJsonPath(featureWithGeometry.getLeft(), jsonPath)
        && isGeometryIntersectingFilterPreparedGeometry(featureWithGeometry.getRight(), preparedGeometry);
  }

  private static boolean isGeometryIntersectingFilterPreparedGeometry(Geometry featureGeometry, PreparedGeometry preparedGeometry) {
    if (featureGeometry == null || featureGeometry.getJTSGeometry() == null || preparedGeometry == null) {
      return false;
    }
    org.locationtech.jts.geom.Geometry jtsGeometry = featureGeometry.getJTSGeometry();
    return jtsGeometry.isValid() && preparedGeometry.intersects(jtsGeometry);
  }
}
