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

package com.here.xyz.hub.rest;

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.EXTENSION;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import static com.here.xyz.util.service.BaseHttpServerVerticle.HeaderValues.APPLICATION_GEO_JSON;
import static io.restassured.RestAssured.given;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * A paged iterate of a composite space has to return the same features as an unpaged one, for composites pinned to a base
 * version and unpinned ones, with one and two levels, in all contexts.
 */
public class CompositePagedIterateIT extends TestSpaceWithFeature {
  private static final String BASE = "composite-paged-iterate" + TEST_SUFFIX;
  private static final String PINNED = BASE + "-pinned";
  private static final String UNPINNED = BASE + "-unpinned";
  private static final String PINNED_INTERMEDIATE = BASE + "-pinned-intermediate";
  private static final String UNPINNED_INTERMEDIATE = BASE + "-unpinned-intermediate";
  private static final String PINNED_2_LEVEL = PINNED_INTERMEDIATE + "-pinned";
  private static final String UNPINNED_2_LEVEL = UNPINNED_INTERMEDIATE + "-unpinned";
  private static final int PAGE_LIMIT = 7;
  private static final int UNPAGED_LIMIT = 1000;

  //The latest version of each composite
  private static final Map<String, Long> versions = new HashMap<>();

  @BeforeAll
  public static void setup() {
    tearDown();

    createSpaceWithId(BASE, 100);
    List<JsonObject> features = new ArrayList<>();
    for (int i = 0; i < 40; i++)
      features.add(feature("f-" + i, i));
    putFeatures(BASE, features);
    List<JsonObject> updates = new ArrayList<>();
    for (int i = 0; i < 10; i++)
      updates.add(feature("f-" + i, i + 100));
    putFeatures(BASE, updates);
    for (int i = 30; i < 35; i++)
      deleteFeature(BASE, "f-" + i);
    long pinnedBaseVersion = putFeatures(BASE, List.of(feature("f-10", 110)));

    createComposite(PINNED, BASE, pinnedBaseVersion);
    createComposite(UNPINNED, BASE, null);
    for (String composite : List.of(PINNED, UNPINNED)) {
      deleteFeature(composite, "f-5");
      writeExtension(composite, "ext-", 11, "f-6");
    }

    //The intermediates hide nothing: a feature hidden in the intermediate shows up in the 2-level composite as a deleted feature,
    //paged or not, which is a different issue
    createComposite(PINNED_INTERMEDIATE, BASE, pinnedBaseVersion);
    createComposite(UNPINNED_INTERMEDIATE, BASE, null);
    for (String intermediate : List.of(PINNED_INTERMEDIATE, UNPINNED_INTERMEDIATE))
      writeExtension(intermediate, "mid-", 5, "f-6");

    createComposite(PINNED_2_LEVEL, PINNED_INTERMEDIATE, versions.get(PINNED_INTERMEDIATE));
    createComposite(UNPINNED_2_LEVEL, UNPINNED_INTERMEDIATE, null);
    for (String composite : List.of(PINNED_2_LEVEL, UNPINNED_2_LEVEL)) {
      deleteFeature(composite, "f-7");
      deleteFeature(composite, "mid-2");
      writeExtension(composite, "ext2-", 10, "f-8", "mid-1");
    }

    //Changes after the pins, the pinned composites must not see them
    putFeatures(PINNED_INTERMEDIATE, List.of(feature("mid-5", 1005)));
    putFeatures(UNPINNED_INTERMEDIATE, List.of(feature("mid-5", 1005)));
    List<JsonObject> laterBaseFeatures = new ArrayList<>();
    for (int i = 40; i < 45; i++)
      laterBaseFeatures.add(feature("f-" + i, i));
    putFeatures(BASE, laterBaseFeatures);
    deleteFeature(BASE, "f-0");
  }

  @AfterAll
  public static void tearDown() {
    for (String spaceId : List.of(PINNED_2_LEVEL, UNPINNED_2_LEVEL, PINNED_INTERMEDIATE, UNPINNED_INTERMEDIATE, PINNED, UNPINNED, BASE))
      removeSpace(spaceId);
  }

  static Stream<Arguments> reads() {
    /*
    composite, context, read at the composite's latest version explicitly, expected number of features
    The base has 35 features at the pinned version and 39 at HEAD. EXTENSION includes the deleted features.
     */
    return Stream.of(
        Arguments.of(PINNED, DEFAULT, false, 45),
        Arguments.of(PINNED, DEFAULT, true, 45),
        Arguments.of(PINNED, EXTENSION, false, 13),
        Arguments.of(PINNED, SUPER, false, 35),
        Arguments.of(UNPINNED, DEFAULT, false, 49),
        Arguments.of(UNPINNED, DEFAULT, true, 49),
        Arguments.of(UNPINNED, EXTENSION, false, 13),
        Arguments.of(UNPINNED, SUPER, false, 39),
        Arguments.of(PINNED_2_LEVEL, DEFAULT, false, 48),
        Arguments.of(PINNED_2_LEVEL, DEFAULT, true, 48),
        Arguments.of(PINNED_2_LEVEL, EXTENSION, false, 14),
        Arguments.of(PINNED_2_LEVEL, SUPER, false, 40),
        Arguments.of(UNPINNED_2_LEVEL, DEFAULT, false, 53),
        Arguments.of(UNPINNED_2_LEVEL, DEFAULT, true, 53),
        Arguments.of(UNPINNED_2_LEVEL, EXTENSION, false, 14),
        Arguments.of(UNPINNED_2_LEVEL, SUPER, false, 45));
  }

  @ParameterizedTest(name = "{0} {1} explicitVersion={2}")
  @MethodSource("reads")
  public void pagedIterateReturnsAllFeatures(String spaceId, SpaceContext context, boolean explicitVersion, int expectedCount) {
    Long version = explicitVersion ? versions.get(spaceId) : null;
    Map<String, JsonObject> unpaged = iterate(spaceId, context, version, UNPAGED_LIMIT);
    assertEquals(expectedCount, unpaged.size(), "Unpaged iterate of " + spaceId + " " + context + ": " + unpaged.keySet());

    Map<String, JsonObject> paged = iterate(spaceId, context, version, PAGE_LIMIT);
    assertEquals(unpaged.keySet(), paged.keySet(), "Paged iterate of " + spaceId + " " + context);
    assertEquals(unpaged, paged, "Paged iterate of " + spaceId + " " + context);
  }

  /**
   * Iterates following the page tokens until there is none.
   *
   * @return The features by ID
   */
  private static Map<String, JsonObject> iterate(String spaceId, SpaceContext context, Long version, int limit) {
    Map<String, JsonObject> features = new TreeMap<>();
    String token = null;
    int pages = 0;
    do {
      RequestSpecification request = given()
          .accept(APPLICATION_GEO_JSON)
          .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
          .queryParam("skipCache", true)
          .queryParam("context", context)
          .queryParam("limit", limit);
      if (version != null)
        request.queryParam("versionRef", version);
      if (token != null)
        request.queryParam("handle", token);
      JsonObject page = new JsonObject(request
          .when()
          .get(getSpacesPath() + "/" + spaceId + "/iterate")
          .then()
          .statusCode(200)
          .extract()
          .asString());

      JsonArray pageFeatures = page.getJsonArray("features");
      assertTrue(pageFeatures.size() <= limit, "Page " + pages + " of " + spaceId + " has " + pageFeatures.size() + " features");
      for (int i = 0; i < pageFeatures.size(); i++) {
        JsonObject feature = pageFeatures.getJsonObject(i);
        assertNull(features.put(feature.getString("id"), feature), "Duplicate feature " + feature.getString("id") + " on page " + pages
            + " of " + spaceId);
      }
      token = page.getString("nextPageToken");
      pages++;
    }
    while (token != null && pages < 100);
    assertNull(token, "Too many pages for " + spaceId);
    return features;
  }

  private static void createComposite(String spaceId, String baseSpaceId, Long baseVersion) {
    JsonObject extendsConfig = new JsonObject().put("spaceId", baseSpaceId);
    if (baseVersion != null)
      extendsConfig.put("version", baseVersion);
    createSpace(new JsonObject()
        .put("id", spaceId)
        .put("title", "Composite paged iterate")
        .put("versionsToKeep", 100)
        .put("extends", extendsConfig)
        .encode());
  }

  /**
   * Adds new features and overrides inherited ones in one write.
   */
  private static void writeExtension(String spaceId, String newIdPrefix, int newCount, String... overriddenIds) {
    List<JsonObject> features = new ArrayList<>();
    for (int i = 0; i < newCount; i++)
      features.add(feature(newIdPrefix + i, 1000 + i));
    for (String id : overriddenIds)
      features.add(feature(id, 2000));
    versions.put(spaceId, putFeatures(spaceId, features));
  }

  private static JsonObject feature(String id, int n) {
    return new JsonObject()
        .put("type", "Feature")
        .put("id", id)
        .put("geometry", new JsonObject().put("type", "Point").put("coordinates", new JsonArray().add(8.0 + n * 0.001).add(50.0)))
        .put("properties", new JsonObject().put("n", n));
  }

  /**
   * @return The version of the write
   */
  private static long putFeatures(String spaceId, List<JsonObject> features) {
    String body = given()
        .contentType(APPLICATION_GEO_JSON)
        .accept(APPLICATION_GEO_JSON)
        .headers(getAuthHeaders(AuthProfile.ACCESS_ALL))
        .body(new JsonObject().put("type", "FeatureCollection").put("features", new JsonArray(features)).encode())
        .when()
        .put(getSpacesPath() + "/" + spaceId + "/features")
        .then()
        .statusCode(200)
        .extract()
        .asString();
    return new JsonObject(body).getJsonArray("features").getJsonObject(0).getJsonObject("properties")
        .getJsonObject("@ns:com:here:xyz").getLong("version");
  }
}
