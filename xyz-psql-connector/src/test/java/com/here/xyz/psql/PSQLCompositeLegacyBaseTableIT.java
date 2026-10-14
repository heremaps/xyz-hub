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

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PSQLCompositeLegacyBaseTableIT extends PSQLAbstractIT {

  private static final String BASE_SPACE_ID = "legacy-composite-base";
  private static final String EXTENSION_SPACE_ID = "legacy-composite-extension";
  private static final String EXTENSION_TABLE = "legacycompositeextensiontable";
  private static final Map<String, Object> CONNECTOR_PARAMS = Map.of(
      CONNECTOR_ID, "test-connector",
      ENABLE_HASHED_SPACEID, true);

  @BeforeAll
  public static void init() throws Exception {
    initEnv(CONNECTOR_PARAMS);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    invokeDeleteTestSpace(CONNECTOR_PARAMS, EXTENSION_SPACE_ID, Map.of(TABLE_NAME, EXTENSION_TABLE));
    invokeDeleteTestSpace(CONNECTOR_PARAMS, BASE_SPACE_ID, null);
  }

  @Test
  void deletingThroughACompositeHidesTheFeatureOfALegacyBase() throws Exception {
    invokeCreateTestSpace(CONNECTOR_PARAMS, BASE_SPACE_ID);
    writeFeature(BASE_SPACE_ID, "base-feature");
    invokeCreateTestSpace(CONNECTOR_PARAMS, new Space().withId(EXTENSION_SPACE_ID), Map.of(TABLE_NAME, EXTENSION_TABLE));

    Map<String, String> deletes = new HashMap<>();
    deletes.put("base-feature", null);
    assertNotNull(deserializeResponse(invokeLambda(new ModifyFeaturesEvent()
        .withSpace(EXTENSION_SPACE_ID)
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(Map.of(TABLE_NAME, EXTENSION_TABLE, "extends", Map.of("spaceId", BASE_SPACE_ID)))
        .withVersionsToKeep(10)
        .withDeleteFeatures(deletes))));

    //The base feature was found in the legacy base table, so the delete became a deletion marker in the extension
    assertEquals(1, featureCount(EXTENSION_TABLE, "base-feature"));
  }

  private static void writeFeature(String spaceId, String featureId) throws Exception {
    WriteFeaturesEvent event = new WriteFeaturesEvent()
        .withSpace(spaceId)
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(Map.of())
        .withVersionsToKeep(10)
        .withResponseDataExpected(true)
        .withModifications(Set.of(new WriteFeaturesEvent.Modification()
            .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)
            .withFeatureData(new FeatureCollection().withFeatures(List.of(newTestFeature(featureId))))));
    assertNotNull(deserializeResponse(invokeLambda(event)));
  }
}
