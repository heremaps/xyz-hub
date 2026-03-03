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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.models.geojson.implementation.Feature;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonPathFilterUtilsTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private Feature feature;

  @BeforeEach
  void setup() throws IOException, URISyntaxException {
    String featureJsonString = Files.readString(Paths.get(
        JsonPathFilterUtilsTest.class.getResource("/test/feature_example.json").toURI()));
    feature = MAPPER.readValue(featureJsonString, Feature.class);
  }

  @Test
  void filterJsonPathSuccessfulSimple() {
    JsonPath jsonPath = JsonPath.compile("$.properties.name");
    boolean isPresent = JsonPathFilterUtils.filterByJsonPath(feature, jsonPath);
    assertTrue(isPresent);
  }

  @Test
  void filterJsonPathSuccessfulConjunction() {
    JsonPath jsonPath = JsonPath.compile("$[?(@.properties.occupant == 'Liverpool F.C.' && @.properties.sport == 'association football')]");
    boolean isPresent = JsonPathFilterUtils.filterByJsonPath(feature, jsonPath);
    assertTrue(isPresent);
  }

  @Test
  void filterJsonPathFailed() {
    JsonPath jsonPath = JsonPath.compile("$.properties.empty");
    boolean isPresent = JsonPathFilterUtils.filterByJsonPath(feature, jsonPath);
    assertFalse(isPresent);
  }

  @Test
  void filterJsonPathInvalid() {
    JsonPath jsonPath = JsonPath.compile("INVALID_JSON_PATH");
    boolean isPresent = JsonPathFilterUtils.filterByJsonPath(feature, jsonPath);
    assertFalse(isPresent);
  }

  @Test
  void filterJsonPathNullFeature() {
    JsonPath jsonPath = JsonPath.compile("$.properties.name");
    assertFalse(JsonPathFilterUtils.filterByJsonPath(null, jsonPath));
  }

  @Test
  void filterJsonPathNullJsonPath() {
    assertTrue(JsonPathFilterUtils.filterByJsonPath(feature, null));
  }

  @Test
  void filterJsonPathInvalidJsonPath() {
    boolean isPresent = JsonPathFilterUtils.filterByJsonPath(feature, JsonPath.compile("$[?(@.id == 'empty')]"));
    assertFalse(isPresent);
  }

  @Test
  void filterJsonPathJsonPathTargetMap() {
    JsonPath jsonPath = JsonPath.compile("$.properties['@ns:com:here:maphub']");
    boolean isPresent = JsonPathFilterUtils.filterByJsonPath(feature, jsonPath);
    assertTrue(isPresent);
  }

  @Test
  void filterJsonPathJsonPathTargetArray() {
    JsonPath jsonPath = JsonPath.compile("$.properties['@ns:com:here:xyz'].tags");
    boolean isPresent = JsonPathFilterUtils.filterByJsonPath(feature, jsonPath);
    assertTrue(isPresent);
  }
}