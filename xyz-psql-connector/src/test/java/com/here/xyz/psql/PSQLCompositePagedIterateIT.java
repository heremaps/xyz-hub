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

package com.here.xyz.psql;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.Random;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A paged iterate of a composite has to continue in the datasets after the one the previous page ended in.
 */
class PSQLCompositePagedIterateIT extends PSQLAbstractIT {
  private static final String PREFIX = "composite_paged_iterate_" + Random.randomAlpha(5).toLowerCase() + "_";
  private static final String BASE = PREFIX + "base";
  private static final String EXTENSION = PREFIX + "ext";
  private static final String EXTENSION_2_LEVEL = PREFIX + "ext_ext";
  private static final int VERSIONS_TO_KEEP = 10;
  private static final int PAGE_LIMIT = 4;
  private static long pinnedBaseVersion;
  private static long extensionVersion;

  @BeforeAll
  public static void setup() throws Exception {
    for (String spaceId : List.of(BASE, EXTENSION, EXTENSION_2_LEVEL))
      invokeCreateTestSpace(null, new Space().withId(spaceId));

    List<Feature> baseFeatures = new ArrayList<>();
    for (int i = 0; i < 20; i++)
      baseFeatures.add(newTestFeature("b-" + i));
    write(BASE, null, baseFeatures);
    write(BASE, null, List.of(newTestFeature("b-0"), newTestFeature("b-1")));
    pinnedBaseVersion = delete(BASE, null, "b-10");
    //After the pinned base version
    write(BASE, null, List.of(newTestFeature("b-20"), newTestFeature("b-21")));

    Map<String, Object> extensionParams = extendsParams(BASE, pinnedBaseVersion);
    extensionVersion = write(EXTENSION, extensionParams, List.of(newTestFeature("x-0"), newTestFeature("x-1"), newTestFeature("x-2"),
        newTestFeature("b-1")));

    write(EXTENSION_2_LEVEL, outerParams(), List.of(newTestFeature("y-0"), newTestFeature("y-1"), newTestFeature("x-1")));
    delete(EXTENSION_2_LEVEL, outerParams(), "x-0");
    //After the pinned extension version. The hiding of b-2 stays after it, because a feature hidden in the intermediate shows up
    //in the 2-level composite as a deleted feature, paged or not, which is a different issue.
    write(EXTENSION, extensionParams, List.of(newTestFeature("x-3")));
    delete(EXTENSION, extensionParams, "b-2");
  }

  @AfterAll
  public static void tearDown() throws Exception {
    for (String spaceId : List.of(EXTENSION_2_LEVEL, EXTENSION, BASE))
      invokeDeleteTestSpace(null, spaceId, null);
  }

  static Stream<Arguments> composites() {
    //space, params, expected number of features
    return Stream.of(
        //19 base features, 4 new ones, b-2 hidden
        Arguments.of(EXTENSION, extendsParams(BASE, pinnedBaseVersion), 22),
        //21 base features, 4 new ones, b-2 hidden
        Arguments.of(EXTENSION, extendsParams(BASE, null), 24),
        //22 features of the extension at the pinned version, 2 new ones, x-0 hidden
        Arguments.of(EXTENSION_2_LEVEL, outerParams(), 23));
  }

  private static Map<String, Object> outerParams() {
    Map<String, Object> params = extendsParams(EXTENSION, extensionVersion);
    ((Map<String, Object>) params.get("extends")).put("extends", extendsParams(BASE, pinnedBaseVersion).get("extends"));
    return params;
  }

  @ParameterizedTest
  @MethodSource("composites")
  void pagedIterateReturnsAllFeatures(String spaceId, Map<String, Object> params, int expectedCount) throws Exception {
    Set<String> unpaged = iterate(spaceId, params, 1000);
    assertEquals(expectedCount, unpaged.size(), "Unpaged: " + unpaged);
    assertEquals(unpaged, iterate(spaceId, params, PAGE_LIMIT));
  }

  private static Set<String> iterate(String spaceId, Map<String, Object> params, int limit) throws Exception {
    Set<String> ids = new TreeSet<>();
    String token = null;
    int pages = 0;
    do {
      IterateFeaturesEvent event = new IterateFeaturesEvent().withNextPageToken(token);
      event.withLimit(limit);
      event.withVersionsToKeep(VERSIONS_TO_KEEP);
      event.withSpace(spaceId);
      event.withParams(params);
      FeatureCollection page = deserializeResponse(invokeLambda(event));
      assertTrue(page.getFeatures().size() <= limit);
      for (Feature feature : page.getFeatures())
        assertTrue(ids.add(feature.getId()), "Duplicate feature " + feature.getId() + " on page " + pages);
      token = page.getNextPageToken();
      pages++;
    }
    while (token != null && pages < 100);
    assertNull(token, "Too many pages");
    return ids;
  }

  private static Map<String, Object> extendsParams(String baseSpaceId, Long baseVersion) {
    Map<String, Object> extendsConfig = new HashMap<>();
    extendsConfig.put("spaceId", baseSpaceId);
    if (baseVersion != null)
      extendsConfig.put("version", baseVersion);
    Map<String, Object> params = new HashMap<>();
    params.put("extends", extendsConfig);
    return params;
  }

  /**
   * @return The version of the write
   */
  private static long write(String spaceId, Map<String, Object> params, List<Feature> features) throws Exception {
    return write(spaceId, params, features, UpdateStrategy.DEFAULT_UPDATE_STRATEGY).getFeatures().get(0).getProperties()
        .getXyzNamespace().getVersion();
  }

  /**
   * @return The version of the deletion
   */
  private static long delete(String spaceId, Map<String, Object> params, String featureId) throws Exception {
    write(spaceId, params, List.of(newTestFeature(featureId)), UpdateStrategy.DEFAULT_DELETE_STRATEGY);
    try (var connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        var statement = connection.createStatement();
        var result = statement.executeQuery("SELECT max(version) FROM public.\"" + spaceId + "\"")) {
      assertTrue(result.next());
      return result.getLong(1);
    }
  }

  private static FeatureCollection write(String spaceId, Map<String, Object> params, List<Feature> features,
      UpdateStrategy updateStrategy) throws Exception {
    return deserializeResponse(invokeLambda(new WriteFeaturesEvent()
        .withSpace(spaceId)
        .withParams(params == null ? Map.of() : params)
        .withVersionsToKeep(VERSIONS_TO_KEEP)
        .withResponseDataExpected(true)
        .withModifications(Set.of(new WriteFeaturesEvent.Modification()
            .withUpdateStrategy(updateStrategy)
            .withFeatureData(new FeatureCollection().withFeatures(features))))));
  }
}
