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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.XyzResponse;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Compares the response of the space's own connector with the response of the Iceberg connector for the same read.
 * Features are matched by ID and compared as JSON trees, ignoring the order of the features and of the object keys and comparing
 * numbers by their decimal value.
 */
public final class IcebergShadowComparison {

  static final int MAX_LISTED_IDS = 10;
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Comparator<JsonNode> NUMERIC_COMPARATOR = (left, right) -> {
    if (left.isNumber() && right.isNumber())
      return left.decimalValue().compareTo(right.decimalValue()) == 0 ? 0 : 1;
    return left.equals(right) ? 0 : 1;
  };

  public enum Outcome {
    MATCH,
    MISMATCH,
    INCOMPARABLE
  }

  /**
   * @param outcome The outcome of the comparison
   * @param reason Why the responses are not comparable or which difference was found, {@code null} for a match
   * @param primaryCount The number of features of the primary response, -1 if unknown
   * @param icebergCount The number of features of the Iceberg response, -1 if unknown
   * @param missing Up to {@value #MAX_LISTED_IDS} IDs of features which only the primary response contains
   * @param extra Up to {@value #MAX_LISTED_IDS} IDs of features which only the Iceberg response contains
   * @param differing Up to {@value #MAX_LISTED_IDS} IDs of features which differ between the responses
   */
  public record Result(Outcome outcome, String reason, int primaryCount, int icebergCount, List<String> missing, List<String> extra,
      List<String> differing) {

    static Result incomparable(String reason, int primaryCount, int icebergCount) {
      return new Result(Outcome.INCOMPARABLE, reason, primaryCount, icebergCount, List.of(), List.of(), List.of());
    }
  }

  private IcebergShadowComparison() {}

  /**
   * @param limit The limit of the read; a response with that many features may be truncated, so it is not compared
   */
  public static Result compare(XyzResponse<?> primary, XyzResponse<?> iceberg, long limit) {
    if (!(primary instanceof FeatureCollection primaryFc))
      return Result.incomparable("primary response " + typeOf(primary), -1, -1);
    if (!(iceberg instanceof FeatureCollection icebergFc))
      return Result.incomparable("iceberg response " + typeOf(iceberg), -1, -1);
    if (hasToken(primaryFc) || hasToken(icebergFc))
      return Result.incomparable("paged response", -1, -1);

    JsonNode primaryFeatures;
    JsonNode icebergFeatures;
    try {
      primaryFeatures = features(primaryFc);
      icebergFeatures = features(icebergFc);
    }
    catch (Exception e) {
      return Result.incomparable("unreadable response: " + e.getMessage(), -1, -1);
    }
    int primaryCount = primaryFeatures.size();
    int icebergCount = icebergFeatures.size();
    if (limit > 0 && (primaryCount >= limit || icebergCount >= limit))
      return Result.incomparable("limit reached", primaryCount, icebergCount);

    Map<String, JsonNode> primaryById = byId(primaryFeatures);
    Map<String, JsonNode> icebergById = byId(icebergFeatures);
    if (primaryById == null || icebergById == null)
      return Result.incomparable("duplicate or missing feature IDs", primaryCount, icebergCount);

    List<String> missing = new ArrayList<>();
    List<String> extra = new ArrayList<>();
    List<String> differing = new ArrayList<>();
    int missingCount = 0, extraCount = 0, differingCount = 0;
    for (Entry<String, JsonNode> entry : primaryById.entrySet()) {
      JsonNode other = icebergById.get(entry.getKey());
      if (other == null)
        missingCount = add(missing, missingCount, entry.getKey());
      else if (!entry.getValue().equals(NUMERIC_COMPARATOR, other))
        differingCount = add(differing, differingCount, entry.getKey());
    }
    for (String id : icebergById.keySet())
      if (!primaryById.containsKey(id))
        extraCount = add(extra, extraCount, id);

    if (missingCount == 0 && extraCount == 0 && differingCount == 0)
      return new Result(Outcome.MATCH, null, primaryCount, icebergCount, List.of(), List.of(), List.of());
    String reason = "missing=" + missingCount + " extra=" + extraCount + " differing=" + differingCount;
    return new Result(Outcome.MISMATCH, reason, primaryCount, icebergCount, List.copyOf(missing), List.copyOf(extra),
        List.copyOf(differing));
  }

  private static int add(List<String> ids, int count, String id) {
    if (ids.size() < MAX_LISTED_IDS)
      ids.add(id);
    return count + 1;
  }

  private static boolean hasToken(FeatureCollection fc) {
    return fc.getNextPageToken() != null || fc.getHandle() != null;
  }

  private static JsonNode features(FeatureCollection fc) throws Exception {
    JsonNode features = MAPPER.readTree(XyzSerializable.serialize(fc)).path("features");
    if (!features.isArray())
      throw new IllegalArgumentException("no features array");
    return features;
  }

  /**
   * @return The features by ID, or {@code null} if a feature has no ID or an ID is not unique
   */
  private static Map<String, JsonNode> byId(JsonNode features) {
    Map<String, JsonNode> byId = new LinkedHashMap<>();
    for (JsonNode feature : features) {
      JsonNode id = feature.get("id");
      if (id == null || id.isNull() || byId.put(id.asText(), feature) != null)
        return null;
    }
    return byId;
  }

  private static String typeOf(XyzResponse<?> response) {
    return response == null ? "null" : response.getClass().getSimpleName();
  }
}
