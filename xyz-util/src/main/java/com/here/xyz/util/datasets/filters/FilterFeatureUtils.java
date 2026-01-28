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
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import com.jayway.jsonpath.JsonPath;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
      logger.debug("Provided null features and geometries collection when filtering features");
      return Collections.emptyList();
    }
    if (spatialFilter == null) {
      logger.debug("Provided null spatial filter when filtering features");
      return filterFeatures(featuresWithGeometries.stream().filter(Objects::nonNull).map(Pair::getLeft).toList(), jsonPaths);
    }
    Collection<JsonPath> compiledJsonPaths = jsonPaths == null ? Collections.emptyList() : jsonPaths.stream()
        .filter(Objects::nonNull)
        .map(JsonPath::compile)
        .toList();
    try {
      spatialFilter.validateSpatialFilter();
      PreparedGeometry preparedGeometry = GEOMETRY_FACTORY.create(spatialFilter.getGeometry().getJTSGeometry());
      return featuresWithGeometries.stream()
          .filter(Objects::nonNull)
          .filter(featureGeometryPair -> filterFeatureWithJsonPathsAndGeometries(featureGeometryPair, compiledJsonPaths, preparedGeometry))
          .map(Pair::getLeft)
          .toList();
    } catch (ValidationException e) {
      logger.warn("Spatial filter validation failed: {}", e.getMessage());
      return Collections.emptyList();
    }
  }

  public static Collection<Feature> filterFeatures(Collection<Feature> features, Collection<String> jsonPaths) {
    if (features == null) {
      logger.debug("Provided null feature collection when filtering features");
      return Collections.emptyList();
    }
    if (jsonPaths == null || jsonPaths.isEmpty()) {
      return features;
    }
    Collection<JsonPath> compiledJsonPaths = jsonPaths.stream()
        .filter(Objects::nonNull)
        .map(JsonPath::compile)
        .toList();
    return features.stream()
        .filter(Objects::nonNull)
        .filter(feature -> filterFeatureByJsonPath(feature, compiledJsonPaths))
        .toList();
  }

  public static boolean filterFeature(Feature feature, Collection<String> jsonPathStrings) {
    if (feature == null) {
      logger.debug("Provided null feature when filtering feature");
      return false;
    }
    if (jsonPathStrings == null || jsonPathStrings.isEmpty()) {
      return true;
    }
    Collection<JsonPath> jsonPaths = jsonPathStrings.stream()
        .filter(Objects::nonNull)
        .map(JsonPath::compile)
        .toList();
    return JsonPathFilterUtils.filterByJsonPaths(feature, jsonPaths);
  }

  private static boolean filterFeatureByJsonPath(Feature feature, Collection<JsonPath> jsonPaths) {
    return JsonPathFilterUtils.filterByJsonPaths(feature, jsonPaths);
  }

  private static boolean filterFeatureWithJsonPathsAndGeometries(Pair<Feature, Geometry> featureWithGeometry,
      Collection<JsonPath> jsonPaths,
      PreparedGeometry preparedGeometry) {
    if (featureWithGeometry == null) {
      logger.debug("Provided null feature and geometry pair when filtering feature");
      return false;
    }
    return filterFeatureByJsonPath(featureWithGeometry.getLeft(), jsonPaths)
        && isGeometryIntersectingSpatialFilterGeometry(featureWithGeometry.getRight(), preparedGeometry);
  }

  private static boolean isGeometryIntersectingSpatialFilterGeometry(Geometry featureGeometry, PreparedGeometry preparedGeometry) {
    if (featureGeometry == null || featureGeometry.getJTSGeometry() == null || preparedGeometry == null) {
      return false;
    }
    org.locationtech.jts.geom.Geometry jtsGeometry = featureGeometry.getJTSGeometry();
    return jtsGeometry.isValid() && preparedGeometry.intersects(jtsGeometry);
  }
}
