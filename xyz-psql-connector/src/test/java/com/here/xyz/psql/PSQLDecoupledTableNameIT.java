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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.here.xyz.events.GetFeaturesByTileEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class PSQLDecoupledTableNameIT extends PSQLAbstractIT {

  private static final String SPACE_ID = "recreated-space";
  private static final String FIRST_TABLE = "firsttable";
  private static final String SECOND_TABLE = "secondtable";
  private static final String BASE_SPACE_ID = "composite-base";
  private static final String EXTENSION_SPACE_ID = "composite-extension";
  private static final String BASE_TABLE = "compositebasetable";
  private static final String EXTENSION_TABLE = "compositeextensiontable";
  private static final Map<String, Object> CONNECTOR_PARAMS = Map.of(
      CONNECTOR_ID, "test-connector",
      ENABLE_HASHED_SPACEID, true);

  @BeforeAll
  public static void init() throws Exception {
    initEnv(CONNECTOR_PARAMS);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    deletePhysicalTable(FIRST_TABLE);
    deletePhysicalTable(SECOND_TABLE);
    deleteSpace(EXTENSION_SPACE_ID, tableParams(EXTENSION_TABLE));
    deleteSpace(BASE_SPACE_ID, tableParams(BASE_TABLE));
    deleteSpace(BASE_SPACE_ID, Map.of()); //The legacy base
  }

  @Test
  void deletingThroughACompositeHidesTheFeatureOfADecoupledBaseTable() throws Exception {
    createSpace(BASE_SPACE_ID, tableParams(BASE_TABLE));
    writeFeature(BASE_SPACE_ID, tableParams(BASE_TABLE), "base-feature");
    createSpace(EXTENSION_SPACE_ID, tableParams(EXTENSION_TABLE));

    deleteThroughComposite(Map.of(TABLE_NAME, EXTENSION_TABLE,
        "extends", Map.of("spaceId", BASE_SPACE_ID, TABLE_NAME, BASE_TABLE)), "base-feature");

    //The base feature was found in the decoupled base table, so the delete became a deletion marker in the extension
    Assertions.assertEquals(1, featureCount(EXTENSION_TABLE, "base-feature"));
  }

  @Test
  void deletingThroughACompositeHidesTheFeatureOfALegacyBaseTable() throws Exception {
    //A legacy base has no explicit table name: its table is derived from the space ID, and "extends" carries no tableName
    createSpace(BASE_SPACE_ID, Map.of());
    writeFeature(BASE_SPACE_ID, Map.of(), "base-feature");
    createSpace(EXTENSION_SPACE_ID, tableParams(EXTENSION_TABLE));

    deleteThroughComposite(Map.of(TABLE_NAME, EXTENSION_TABLE,
        "extends", Map.of("spaceId", BASE_SPACE_ID)), "base-feature");

    Assertions.assertEquals(1, featureCount(EXTENSION_TABLE, "base-feature"));
  }

  @Test
  void createsTwoPhysicalTablesForTheSameLogicalSpace() throws Exception {
    createPhysicalTable(FIRST_TABLE);
    writeFeature(FIRST_TABLE, "first-feature");
    createPhysicalTable(SECOND_TABLE);
    writeFeature(SECOND_TABLE, "second-feature");

    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        Statement statement = connection.createStatement();
        ResultSet tables = statement.executeQuery("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('firsttable', 'secondtable')
            """)) {
      int tableCount = 0;
      while (tables.next())
        tableCount++;
      Assertions.assertEquals(2, tableCount);
    }

    Assertions.assertEquals(1, featureCount(FIRST_TABLE, "first-feature"));
    Assertions.assertEquals(0, featureCount(FIRST_TABLE, "second-feature"));
    Assertions.assertEquals(0, featureCount(SECOND_TABLE, "first-feature"));
    Assertions.assertEquals(1, featureCount(SECOND_TABLE, "second-feature"));

    assertMetadataTable(SECOND_TABLE);

    //Simulate a delayed deletion event for the previous incarnation after the new incarnation is already active
    deletePhysicalTable(FIRST_TABLE);

    Assertions.assertFalse(tableExists(FIRST_TABLE));
    Assertions.assertTrue(tableExists(SECOND_TABLE));
    Assertions.assertEquals(1, featureCount(SECOND_TABLE, "second-feature"));
    writeFeature(SECOND_TABLE, "feature-after-delayed-delete");
    Assertions.assertEquals(1, featureCount(SECOND_TABLE, "feature-after-delayed-delete"));
    assertMetadataTable(SECOND_TABLE);
  }

  @Test
  void vizSamplingUsesTheCurrentPhysicalTableAfterSpaceRecreation() throws Exception {
    createPhysicalTable(FIRST_TABLE);
    assertVizReadSucceeds(FIRST_TABLE);

    deletePhysicalTable(FIRST_TABLE);
    createPhysicalTable(SECOND_TABLE);

    assertVizReadSucceeds(SECOND_TABLE);
  }

  private static void assertVizReadSucceeds(String tableName) throws Exception {
    GetFeaturesByTileEvent event = new GetFeaturesByTileEvent()
        .withSpace(SPACE_ID)
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(Map.of(TABLE_NAME, tableName))
        .withLevel(0)
        .withX(0)
        .withY(0)
        .withBbox(new BBox(-180, -85, 180, 85))
        .withOptimizationMode("viz")
        .withVizSampling("low");

    FeatureCollection response = deserializeResponse(invokeLambda(event));
    Assertions.assertNotNull(response);
  }

  private static Map<String, Object> tableParams(String tableName) {
    return Map.of(TABLE_NAME, tableName);
  }

  private static void createSpace(String spaceId, Map<String, Object> params) throws Exception {
    invokeLambda(new ModifySpaceEvent()
        .withSpace(spaceId)
        .withOperation(ModifySpaceEvent.Operation.CREATE)
        .withSpaceDefinition(new Space().withId(spaceId))
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(params));
  }

  private static void deleteSpace(String spaceId, Map<String, Object> params) throws Exception {
    invokeLambda(new ModifySpaceEvent()
        .withSpace(spaceId)
        .withOperation(ModifySpaceEvent.Operation.DELETE)
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(params));
  }

  private static void writeFeature(String spaceId, Map<String, Object> params, String featureId) throws Exception {
    WriteFeaturesEvent event = new WriteFeaturesEvent()
        .withSpace(spaceId)
        //Without a tableName the table is derived from the space ID, hashed as the connector params say
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(params)
        .withVersionsToKeep(10)
        .withResponseDataExpected(true)
        .withModifications(Set.of(new WriteFeaturesEvent.Modification()
            .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)
            .withFeatureData(new FeatureCollection().withFeatures(List.of(newTestFeature(featureId))))));
    Assertions.assertNotNull(deserializeResponse(invokeLambda(event)));
  }

  private static void deleteThroughComposite(Map<String, Object> compositeParams, String featureId) throws Exception {
    Map<String, String> deletes = new HashMap<>();
    deletes.put(featureId, null);
    Assertions.assertNotNull(deserializeResponse(invokeLambda(new ModifyFeaturesEvent()
        .withSpace(EXTENSION_SPACE_ID)
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(compositeParams)
        .withVersionsToKeep(10)
        .withDeleteFeatures(deletes))));
  }

  private static void createPhysicalTable(String tableName) throws Exception {
    ModifySpaceEvent event = new ModifySpaceEvent()
        .withSpace(SPACE_ID)
        .withOperation(ModifySpaceEvent.Operation.CREATE)
        .withSpaceDefinition(new Space().withId(SPACE_ID))
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(Map.of(TABLE_NAME, tableName));
    invokeLambda(event);
  }

  private static void writeFeature(String tableName, String featureId) throws Exception {
    WriteFeaturesEvent event = new WriteFeaturesEvent()
        .withSpace(SPACE_ID)
        .withParams(Map.of(TABLE_NAME, tableName))
        .withVersionsToKeep(1)
        .withResponseDataExpected(true)
        .withModifications(Set.of(new WriteFeaturesEvent.Modification()
            .withUpdateStrategy(UpdateStrategy.DEFAULT_UPDATE_STRATEGY)
            .withFeatureData(new FeatureCollection().withFeatures(List.of(newTestFeature(featureId))))));
    Assertions.assertNotNull(deserializeResponse(invokeLambda(event)));
  }

  private static long featureCount(String tableName, String featureId) throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(
            "SELECT count(*) FROM public.\"" + tableName + "\" WHERE id = '" + featureId + "'")) {
      Assertions.assertTrue(result.next());
      return result.getLong(1);
    }
  }

  private static boolean tableExists(String tableName) throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery("""
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = 'public' AND table_name = '%s'
            )
            """.formatted(tableName))) {
      Assertions.assertTrue(result.next());
      return result.getBoolean(1);
    }
  }

  private static void assertMetadataTable(String expectedTableName) throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        Statement statement = connection.createStatement();
        ResultSet metadata = statement.executeQuery(
            "SELECT h_id FROM xyz_config.space_meta WHERE id = 'recreated-space' AND schem = 'public'")) {
      Assertions.assertTrue(metadata.next());
      Assertions.assertEquals(expectedTableName, metadata.getString("h_id"));
    }
  }

  private static void deletePhysicalTable(String tableName) throws Exception {
    ModifySpaceEvent event = new ModifySpaceEvent()
        .withSpace(SPACE_ID)
        .withOperation(ModifySpaceEvent.Operation.DELETE)
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(Map.of(TABLE_NAME, tableName));
    invokeLambda(event);
  }
}