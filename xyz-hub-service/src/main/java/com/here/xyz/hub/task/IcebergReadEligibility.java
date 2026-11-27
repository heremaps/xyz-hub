/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.hub.task;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;

import com.here.xyz.events.ContextAwareEvent;
import com.here.xyz.events.GetFeaturesByBBoxEvent;
import com.here.xyz.events.GetFeaturesByGeometryEvent;
import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.PropertiesQuery;
import com.here.xyz.events.PropertyQuery;
import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.here.xyz.events.PropertyQueryList;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.hub.Service;
import com.here.xyz.hub.config.IcebergVersionCache.IcebergVersion;
import com.here.xyz.hub.connectors.models.Connector;
import com.here.xyz.hub.rest.ApiParam;
import com.here.xyz.hub.task.FeatureTask.BBoxQuery;
import com.here.xyz.hub.task.FeatureTask.GeometryQuery;
import com.here.xyz.hub.task.FeatureTask.IterateQuery;
import com.here.xyz.hub.task.FeatureTask.SearchQuery;
import com.here.xyz.hub.task.FeatureTask.TileQuery;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.iceberg.IcebergConnectorConfig;
import com.here.xyz.models.hub.Space.Extension;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Decides whether a read can be served by the Iceberg connector with the same result as by the space's own connector.
 */
final class IcebergReadEligibility {

  private static final String RAW_OPTIMIZATION_MODE = "raw";
  static final String BASE_VERSION_PLACEHOLDER = "{baseVersion}";

  private IcebergReadEligibility() {}

  /**
   * The checks which do not depend on the Iceberg connector. The property keys of a properties query are not checked, see
   * {@link #reasonNotEligible(FeatureTask, Set)}.
   *
   * @return The reason why the read of the task can not be served by the Iceberg connector, or {@code null} if it might be
   */
  static String reasonNotEligible(FeatureTask<?, ?> task) {
    return reasonNotEligible(task, null);
  }

  /**
   * @param filterProperties The properties the Iceberg connector can filter by (see {@link #filterProperties(Connector)}), or
   *     {@code null} to not check the property keys yet
   * @return The reason why the read of the task can not be served by the Iceberg connector, or {@code null} if it can
   */
  static String reasonNotEligible(FeatureTask<?, ?> task, Set<String> filterProperties) {
    if (!(task instanceof GeometryQuery || task instanceof BBoxQuery || task instanceof TileQuery || task instanceof IterateQuery
        || task instanceof SearchQuery))
      return "task " + task.getClass().getSimpleName();
    if (task.space == null)
      return "no space";
    if (!isEmpty(task.space.getProcessors()) || !isEmpty(task.space.getListeners()))
      return "processors or listeners";
    if (!(task.getEvent() instanceof SearchForFeaturesEvent<?> event))
      return "event " + task.getEvent().getClass().getSimpleName();
    if (event.getContext() != DEFAULT)
      return "context " + event.getContext();
    //A request with context SUPER was switched to the extended space, see FeatureTaskHandler#switchToSuperSpace
    if (!task.space.getId().equals(task.context.pathParam(ApiParam.Path.SPACE_ID)))
      return "resolved space differs from the requested space";
    if (event.getSelection() != null && !event.getSelection().isEmpty())
      return "selection";
    if (event.isForce2D())
      return "force2D";

    String reason = propertiesQueryReason(task, event, filterProperties);
    if (reason == null)
      reason = eventReason(event);
    if (reason == null)
      reason = compositeReason(task);
    return reason;
  }

  /**
   * @return The properties (property query keys) the Iceberg connector can filter by, from its {@code filterColumns} param
   */
  static Set<String> filterProperties(Connector connector) {
    try {
      return IcebergConnectorConfig.filterColumns(connector.params).keySet();
    }
    catch (IllegalArgumentException e) {
      return Set.of();
    }
  }

  /**
   * The filter for the Iceberg versions which hold the data of the task's space. For a composite space, which extends a pinned
   * base version B, only the versions in tables whose name ends with {@link com.here.xyz.hub.Config#ICEBERG_READS_COMPOSITE_TABLE_SUFFIX}
   * (by default {@code ...__b<B>}) hold the merged data.
   * Must only be called for tasks which are eligible.
   */
  static Predicate<IcebergVersion> compositeFilter(FeatureTask<?, ?> task) {
    Extension extension = task.space.getExtension();
    if (extension == null)
      return v -> true;
    String suffix = compositeTableSuffix().replace(BASE_VERSION_PLACEHOLDER, String.valueOf(extension.getVersion()));
    return v -> v.location().table().endsWith(suffix);
  }

  private static String compositeTableSuffix() {
    String suffix = Service.configuration == null ? null : Service.configuration.ICEBERG_READS_COMPOSITE_TABLE_SUFFIX;
    return suffix == null ? "" : suffix.trim();
  }

  private static String propertiesQueryReason(FeatureTask<?, ?> task, SearchForFeaturesEvent<?> event, Set<String> filterProperties) {
    PropertiesQuery propertiesQuery = event.getPropertiesQuery();
    if (task instanceof SearchQuery) {
      if (event.getClass() != SearchForFeaturesEvent.class)
        return "event " + event.getClass().getSimpleName();
      if (propertiesQuery == null || propertiesQuery.isEmpty())
        return "search without a properties query";
    }
    if (propertiesQuery == null || propertiesQuery.isEmpty())
      return null;
    if (filterProperties == null)
      return isEqualsOnly(propertiesQuery, null) ? null : "properties query other than equals with string values";
    if (filterProperties.isEmpty())
      return "properties query, the Iceberg connector has no filterColumns";
    if (!isEqualsOnly(propertiesQuery, filterProperties))
      return "properties query other than " + filterProperties.stream().sorted().map(key -> "f." + key)
          .collect(Collectors.joining(", "));
    return null;
  }

  /**
   * @param keys The allowed property keys, or {@code null} for any key
   */
  private static boolean isEqualsOnly(PropertiesQuery propertiesQuery, Set<String> keys) {
    for (PropertyQueryList andList : propertiesQuery) {
      if (andList == null || andList.isEmpty())
        return false;
      for (PropertyQuery query : andList) {
        if ((keys != null && !keys.contains(query.getKey())) || query.getOperation() != QueryOperation.EQUALS
            || query.getValues() == null || query.getValues().isEmpty()
            || !query.getValues().stream().allMatch(value -> value instanceof String))
          return false;
      }
    }
    return true;
  }

  private static String eventReason(SearchForFeaturesEvent<?> event) {
    if (event instanceof IterateFeaturesEvent<?> iterate) {
      if (iterate.getStartI() != -1 || iterate.getEndI() != -1)
        return "partitioned iterate";
    }
    else if (event instanceof GetFeaturesByGeometryEvent geometry) {
      if (geometry.getGeometry() == null)
        return "no geometry";
      if (geometry.getH3Index() != null)
        return "h3Index";
      if (isClip(geometry.getClip()))
        return "clip";
    }
    else if (event instanceof GetFeaturesByTileEvent tile) {
      String reason = bboxOptionsReason(tile);
      if (reason != null)
        return reason;
      switch (tile.getResponseType()) {
        case GEO_JSON:
          if (isClip(tile.getClip()))
            return "clip";
          break;
        case MVT:
        case MVT_FLATTENED:
          //The connector answers with GeoJSON, so the Hub would have to encode the MVT itself. Its encoder (mapbox-vector-tile 3.1.0)
          //fails on polygons with the Hub's JTS version, so MVT tiles stay with the space's own connector, which encodes them itself.
          return "MVT tile, encoded by the space's connector";
        default:
          return "tile response type " + tile.getResponseType();
      }
    }
    else if (event.getClass() == GetFeaturesByBBoxEvent.class) {
      GetFeaturesByBBoxEvent<?> bboxEvent = (GetFeaturesByBBoxEvent<?>) event;
      BBox bbox = bboxEvent.getBbox();
      if (bbox == null)
        return "no bbox";
      if (bbox.minLon() > bbox.maxLon())
        return "bbox crossing the antimeridian";
      String reason = bboxOptionsReason(bboxEvent);
      if (reason != null)
        return reason;
      if (isClip(bboxEvent.getClip()))
        return "clip";
    }
    else if (event.getClass() != SearchForFeaturesEvent.class)
      return "event " + event.getClass().getSimpleName();
    return null;
  }

  private static String bboxOptionsReason(GetFeaturesByBBoxEvent<?> event) {
    if (event.getClusteringType() != null)
      return "clustering";
    if (event.getTweakType() != null)
      return "tweaks";
    if (event.getOptimizationMode() != null && !RAW_OPTIMIZATION_MODE.equals(event.getOptimizationMode()))
      return "optimizationMode " + event.getOptimizationMode();
    return null;
  }

  private static String compositeReason(FeatureTask<?, ?> task) {
    Extension extension = task.space.getExtension();
    if (extension == null)
      return null;
    if (task.extendedSpaces == null || task.extendedSpaces.size() != 1)
      return "composite with " + (task.extendedSpaces == null ? 0 : task.extendedSpaces.size()) + " extended spaces";
    //The Hub reads a version E of an unpinned composite with the base as it was when E was written, which no tag records
    if (extension.getVersion() == null)
      return "unpinned composite";
    if (compositeTableSuffix().isEmpty())
      return "composite without ICEBERG_READS_COMPOSITE_TABLE_SUFFIX";
    return null;
  }

  private static boolean isClip(Boolean clip) {
    return Boolean.TRUE.equals(clip);
  }

  private static boolean isEmpty(Map<?, ?> map) {
    return map == null || map.isEmpty();
  }
}
