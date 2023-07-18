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
package com.here.xyz.hub.rest;

import com.here.xyz.hub.task.NakshaTask;
import com.here.xyz.hub.task.feature.GetFeaturesByBBoxTask;
import com.here.xyz.hub.task.feature.GetFeaturesByGeometryTask;
import com.here.xyz.hub.task.feature.GetFeaturesByTileTask;
import com.here.xyz.hub.task.feature.GetStatisticsTask;
import com.here.xyz.hub.task.feature.IterateFeaturesTask;
import com.here.xyz.hub.task.feature.SearchForFeaturesTask;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;

public class FeatureQueryApi extends SpaceBasedApi {

  public FeatureQueryApi(RouterBuilder rb) {
    rb.operation("getFeaturesBySpatial").handler(this::getFeaturesBySpatial);
    rb.operation("getFeaturesBySpatialPost").handler(this::getFeaturesBySpatial);
    rb.operation("getFeaturesByBBox").handler(this::getFeaturesByBBox);
    rb.operation("getFeaturesByTile").handler(this::getFeaturesByTile);
    rb.operation("getFeaturesCount").handler(this::getFeaturesCount);
    rb.operation("getStatistics").handler(this::getStatistics);
    rb.operation("iterateFeatures").handler(this::iterateFeatures);
    rb.operation("searchForFeatures").handler(this::searchForFeatures);
  }

  /**
   * Retrieves the count of features in the space.
   */
  @Deprecated
  private void getFeaturesCount(final RoutingContext context) {
    NakshaTask.start(GetStatisticsTask.class, context, ApiResponseType.COUNT_RESPONSE);
  }

  /**
   * Retrieves the statistics about a space.
   */
  private void getStatistics(final RoutingContext context) {
    NakshaTask.start(GetStatisticsTask.class, context, ApiResponseType.STATISTICS_RESPONSE);
  }

  /**
   * Searches for features by tags.
   */
  private void searchForFeatures(final RoutingContext context) {
    NakshaTask.start(SearchForFeaturesTask.class, context, ApiResponseType.FEATURE_COLLECTION);
  }

  /**
   * Iterate the content of the space.
   */
  private void iterateFeatures(final RoutingContext context) {
    NakshaTask.start(IterateFeaturesTask.class, context, ApiResponseType.FEATURE_COLLECTION);
  }

  /**
   * Retrieves the features by intersecting with the provided geometry.
   */
  private void getFeaturesBySpatial(final RoutingContext context) {
    NakshaTask.start(GetFeaturesByGeometryTask.class, context, ApiResponseType.FEATURE_COLLECTION);
  }

  /**
   * Retrieves the features in the bounding box.
   */
  private void getFeaturesByBBox(final RoutingContext context) {
    NakshaTask.start(GetFeaturesByBBoxTask.class, context, ApiResponseType.FEATURE_COLLECTION);
  }

  /**
   * Retrieves the features in a tile.
   */
  private void getFeaturesByTile(final RoutingContext context) {
    NakshaTask.start(GetFeaturesByTileTask.class, context, ApiResponseType.FEATURE_COLLECTION);
  }
}
