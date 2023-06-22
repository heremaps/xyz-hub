/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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
package com.here.naksha.lib.psql;

import static com.here.naksha.lib.core.NakshaContext.currentLogger;
import static org.junit.jupiter.api.Assertions.*;

import com.amazonaws.util.IOUtils;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PSQLDeleteIT extends PSQLAbstractIT {

  @BeforeAll
  public static void init() throws Exception {
    initEnv(null);
  }

  @AfterAll
  public void shutdown() throws Exception {
    invokeDeleteTestSpace(null);
  }

  @Test
  public void testDeleteFeatures() throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEvent.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    currentLogger().info("RAW RESPONSE: " + insertResponse);

    String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);

    final JsonPath jsonPathFeatureIds = JsonPath.compile("$.features..id");
    List<String> ids = jsonPathFeatureIds.read(insertResponse, jsonPathConf);
    currentLogger().info("Preparation: Inserted features {}", ids);

    // =========== DELETE ==========
    final DocumentContext modifyFeaturesEventDoc = getEventFromResource("/events/InsertFeaturesEvent.json");
    modifyFeaturesEventDoc.delete("$.insertFeatures");

    Map<String, String> idsMap = new HashMap<>();
    ids.forEach(id -> idsMap.put(id, null));
    modifyFeaturesEventDoc.put("$", "deleteFeatures", idsMap);

    String deleteEvent = modifyFeaturesEventDoc.jsonString();
    String deleteResponse = invokeLambda(deleteEvent);
    assertNoErrorInResponse(deleteResponse);
    currentLogger().info("Modify features tested successfully");
  }

  @Test
  public void testDeleteFeaturesByTagWithOldStates() throws Exception {
    testDeleteFeaturesByTag(true);
  }

  @Test
  public void testDeleteFeaturesByTagDefault() throws Exception {
    testDeleteFeaturesByTag(false);
  }

  protected void testDeleteFeaturesByTag(boolean includeOldStates) throws Exception {
    // =========== INSERT ==========
    String insertJsonFile = "/events/InsertFeaturesEvent.json";
    String insertResponse = invokeLambdaFromFile(insertJsonFile);
    currentLogger().info("RAW RESPONSE: " + insertResponse);
    String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
    assertRead(insertRequest, insertResponse, false);
    currentLogger().info("Preparation: Insert features");

    // =========== COUNT ==========
    String statsResponse = invokeLambdaFromFile("/events/GetStatisticsEvent.json");
    Integer originalCount = JsonPath.read(statsResponse, "$.count.value");
    currentLogger().info("Preparation: feature count = {}", originalCount);

    // =========== DELETE SOME TAGGED FEATURES ==========
    currentLogger().info("Delete tagged features");
    final DocumentContext deleteByTagEventDoc = getEventFromResource("/events/DeleteFeaturesByTagEvent.json");
    deleteByTagEventDoc.put("$", "params", Collections.singletonMap("includeOldStates", includeOldStates));
    String[][] tags = {{"yellow"}};
    deleteByTagEventDoc.put("$", "tags", tags);
    String deleteByTagEvent = deleteByTagEventDoc.jsonString();
    String deleteByTagResponse = invokeLambda(deleteByTagEvent);
    assertNoErrorInResponse(deleteByTagResponse);
    final JsonPath jsonPathFeatures = JsonPath.compile("$.features");
    @SuppressWarnings("rawtypes")
    List features = jsonPathFeatures.read(deleteByTagResponse, jsonPathConf);
    if (includeOldStates) {
      assertNotNull(features, "'features' element in DeleteByTagResponse is missing");
      assertTrue(features.size() > 0, "'features' element in DeleteByTagResponse is empty");
    } else if (features != null) {
      assertEquals(0, features.size(), "unexpected features in DeleteByTagResponse");
    }

    statsResponse = invokeLambdaFromFile("/events/GetStatisticsEvent.json");
    Integer count = JsonPath.read(statsResponse, "$.count.value");
    assertTrue(originalCount > count);
    currentLogger().info("Delete tagged features tested successfully");

    // =========== DELETE ALL FEATURES ==========
    deleteByTagEventDoc.put("$", "tags", null);
    String deleteAllEvent = deleteByTagEventDoc.jsonString();
    String deleteAllResponse = invokeLambda(deleteAllEvent);
    assertNoErrorInResponse(deleteAllResponse);
    features = jsonPathFeatures.read(deleteAllResponse, jsonPathConf);
    if (features != null) {
      assertEquals(0, features.size(), "unexpected features in DeleteByTagResponse");
    }

    statsResponse = invokeLambdaFromFile("/events/GetStatisticsEvent.json");
    count = JsonPath.read(statsResponse, "$.count.value");
    assertEquals(0, count.intValue());
    currentLogger().info("Delete all features tested successfully");
  }
}
