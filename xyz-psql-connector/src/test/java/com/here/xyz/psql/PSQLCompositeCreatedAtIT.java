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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.util.db.SQLQuery;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Verifies the creation timestamp which the legacy write path ({@link DatabaseHandler#executeModifyFeatures}) stores when a feature
 * of the base space is written into an extension for the first time. The FeatureWriter and the express import writer are covered by
 * the shared FeatureWriter test suite.
 */
class PSQLCompositeCreatedAtIT extends PSQLAbstractIT {
  private static final String BASE_SPACE_ID = "createdat-composite-base";
  private static final String EXTENSION_SPACE_ID = "createdat-composite-extension";
  private static final String EXTENSION_TABLE = "createdatcompositeextensiontable";
  private static final String BASE_FEATURE_ID = "base-feature";
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
  void updatingABaseFeatureThroughTheExtensionKeepsItsCreatedAt() throws Exception {
    long baseCreatedAt = createSpacesAndBaseFeature();

    modify(10, null, List.of(featureLikeTheHubSendsIt(BASE_FEATURE_ID, baseCreatedAt)), null);

    assertEquals("I", latestOperation(BASE_FEATURE_ID));
    assertEquals(baseCreatedAt, latestCreatedAt(BASE_FEATURE_ID));
    assertNotEquals(latestUpdatedAt(BASE_FEATURE_ID), latestCreatedAt(BASE_FEATURE_ID));

    //A further update finds the feature in the extension already
    modify(10, null, List.of(featureLikeTheHubSendsIt(BASE_FEATURE_ID, baseCreatedAt)), null);

    assertEquals("U", latestOperation(BASE_FEATURE_ID));
    assertEquals(baseCreatedAt, latestCreatedAt(BASE_FEATURE_ID));
  }

  @Test
  void aBaseFeatureWhichIsInsertedAgainKeepsItsCreatedAt() throws Exception {
    long baseCreatedAt = createSpacesAndBaseFeature();

    //Upserts reach the connector as inserts as well
    modify(10, List.of(featureLikeTheHubSendsIt(BASE_FEATURE_ID, 0)), null, null);

    assertEquals(baseCreatedAt, latestCreatedAt(BASE_FEATURE_ID));
  }

  @Test
  void aNewFeatureGetsItsCreatedAtFromTheWrite() throws Exception {
    createSpacesAndBaseFeature();

    modify(10, List.of(featureLikeTheHubSendsIt("new-feature", 0)), null, null);

    assertEquals("I", latestOperation("new-feature"));
    assertEquals(latestUpdatedAt("new-feature"), latestCreatedAt("new-feature"));
  }

  @Test
  void deletingABaseFeatureThroughTheExtensionStaysACreation() throws Exception {
    createSpacesAndBaseFeature();

    Map<String, String> deletes = new HashMap<>();
    deletes.put(BASE_FEATURE_ID, null);
    modify(10, null, null, deletes);

    assertEquals("H", latestOperation(BASE_FEATURE_ID));
    assertEquals(latestUpdatedAt(BASE_FEATURE_ID), latestCreatedAt(BASE_FEATURE_ID));
  }

  @Test
  void withoutHistoryTheCreatedAtIsUnchanged() throws Exception {
    long baseCreatedAt = createSpacesAndBaseFeature();

    modify(1, null, List.of(featureLikeTheHubSendsIt(BASE_FEATURE_ID, baseCreatedAt)), null);

    assertEquals(latestUpdatedAt(BASE_FEATURE_ID), latestCreatedAt(BASE_FEATURE_ID));
  }

  private static long createSpacesAndBaseFeature() throws Exception {
    invokeCreateTestSpace(CONNECTOR_PARAMS, BASE_SPACE_ID);
    long baseCreatedAt = writeBaseFeature();
    invokeCreateTestSpace(CONNECTOR_PARAMS, new Space().withId(EXTENSION_SPACE_ID), Map.of(TABLE_NAME, EXTENSION_TABLE));
    //Make sure that the write timestamps differ from the one of the base feature
    Thread.sleep(50);
    return baseCreatedAt;
  }

  private static long writeBaseFeature() throws Exception {
    WriteFeaturesEvent event = new WriteFeaturesEvent()
        .withSpace(BASE_SPACE_ID)
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(Map.of())
        .withVersionsToKeep(10)
        .withResponseDataExpected(true)
        .withModifications(Set.of(new WriteFeaturesEvent.Modification()
            .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)
            .withFeatureData(new FeatureCollection().withFeatures(List.of(newTestFeature(BASE_FEATURE_ID))))));
    FeatureCollection response = deserializeResponse(invokeLambda(event));
    assertNotNull(response);
    long createdAt = response.getFeatures().get(0).getProperties().getXyzNamespace().getCreatedAt();
    assertNotEquals(0, createdAt);
    return createdAt;
  }

  /**
   * Builds a feature like the Hub does before it calls the connector: The creation timestamp is the one of the visible feature, if
   * there is one, and the update timestamp is the one of the write.
   */
  private static Feature featureLikeTheHubSendsIt(String id, long createdAt) {
    long now = System.currentTimeMillis();
    return new Feature()
        .withId(id)
        .withProperties(new Properties().withXyzNamespace(new XyzNamespace()
            .withCreatedAt(createdAt != 0 ? createdAt : now)
            .withUpdatedAt(now)));
  }

  private static void modify(int versionsToKeep, List<Feature> inserts, List<Feature> updates, Map<String, String> deletes)
      throws Exception {
    assertNotNull(deserializeResponse(invokeLambda(new ModifyFeaturesEvent()
        .withSpace(EXTENSION_SPACE_ID)
        .withContext(SpaceContext.DEFAULT)
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(Map.of(TABLE_NAME, EXTENSION_TABLE, "extends", Map.of("spaceId", BASE_SPACE_ID)))
        .withVersionsToKeep(versionsToKeep)
        //The connector modifies the lists, like it does with the ones it gets from the deserialization of an event
        .withInsertFeatures(inserts == null ? null : new ArrayList<>(inserts))
        .withUpdateFeatures(updates == null ? null : new ArrayList<>(updates))
        .withDeleteFeatures(deletes))));
  }

  private static String latestOperation(String featureId) throws Exception {
    return readLatestRow(featureId, "operation");
  }

  private static long latestCreatedAt(String featureId) throws Exception {
    return Long.parseLong(readLatestRow(featureId, "jsondata->'properties'->'@ns:com:here:xyz'->>'createdAt'"));
  }

  private static long latestUpdatedAt(String featureId) throws Exception {
    return Long.parseLong(readLatestRow(featureId, "jsondata->'properties'->'@ns:com:here:xyz'->>'updatedAt'"));
  }

  private static String readLatestRow(String featureId, String column) throws Exception {
    return new SQLQuery("SELECT ${{column}} AS value FROM ${schema}.${table} WHERE id = #{id} ORDER BY version DESC LIMIT 1")
        .withQueryFragment("column", column)
        .withVariable("schema", "public")
        .withVariable("table", EXTENSION_TABLE)
        .withNamedParameter("id", featureId)
        .run(LAMBDA.dataSourceProvider, rs -> {
          assertEquals(true, rs.next(), "No row found for " + featureId);
          return rs.getString("value");
        });
  }
}
