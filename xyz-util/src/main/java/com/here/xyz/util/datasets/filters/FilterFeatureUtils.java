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
    if (spatialFilter == null) {
      logger.debug("Provided null spatial filter when filtering features");
      return filterFeatures(featuresWithGeometries.stream().filter(Objects::nonNull).map(Pair::getLeft).toList(), jsonPaths);
    }
    try {
      spatialFilter.validateSpatialFilter();
      PreparedGeometry preparedGeometry = GEOMETRY_FACTORY.create(spatialFilter.getGeometry().getJTSGeometry());
      return featuresWithGeometries.stream()
          .filter(Objects::nonNull)
          .filter(featureGeometryPair -> filterFeature(featureGeometryPair, jsonPaths, preparedGeometry))
          .map(Pair::getLeft)
          .toList();
    } catch (ValidationException e) {
      logger.warn("Spatial filter validation failed: {}", e.getMessage());
      return Collections.emptyList();
    }
  }

  public static Collection<Feature> filterFeatures(Collection<Feature> features, Collection<String> jsonPaths) {
    return features.stream()
        .filter(Objects::nonNull)
        .filter(feature -> JsonPathFilterUtils.filterByJsonPaths(feature, jsonPaths))
        .toList();
  }

  public static boolean filterFeature(Feature feature, Collection<String> jsonPaths) {
    return JsonPathFilterUtils.filterByJsonPaths(feature, jsonPaths);
  }

  private static boolean filterFeature(Pair<Feature, Geometry> featureWithGeometry, Collection<String> jsonPaths,
      PreparedGeometry preparedGeometry) {
    return filterFeature(featureWithGeometry.getLeft(), jsonPaths)
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
