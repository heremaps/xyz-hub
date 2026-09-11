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
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.coordinates.BBox;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Space;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class PSQLDecoupledTableNameIT extends PSQLAbstractIT {

  private static final String SPACE_ID = "recreated-space";
  private static final String FIRST_TABLE = "firsttable";
  private static final String SECOND_TABLE = "secondtable";
  private static final Map<String, Object> CONNECTOR_PARAMS = Map.of(
      CONNECTOR_ID, "test-connector",
      ENABLE_HASHED_SPACEID, true);

  @BeforeClass
  public static void init() throws Exception {
    initEnv(CONNECTOR_PARAMS);
  }

  @After
  public void cleanUp() throws Exception {
    deletePhysicalTable(FIRST_TABLE);
    deletePhysicalTable(SECOND_TABLE);
  }

  @Test
  public void createsTwoPhysicalTablesForTheSameLogicalSpace() throws Exception {
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
      assertEquals(2, tableCount);
    }

    assertEquals(1, featureCount(FIRST_TABLE, "first-feature"));
    assertEquals(0, featureCount(FIRST_TABLE, "second-feature"));
    assertEquals(0, featureCount(SECOND_TABLE, "first-feature"));
    assertEquals(1, featureCount(SECOND_TABLE, "second-feature"));

    assertMetadataTable(SECOND_TABLE);

    //Simulate a delayed deletion event for the previous incarnation after the new incarnation is already active
    deletePhysicalTable(FIRST_TABLE);

    assertFalse(tableExists(FIRST_TABLE));
    assertTrue(tableExists(SECOND_TABLE));
    assertEquals(1, featureCount(SECOND_TABLE, "second-feature"));
    writeFeature(SECOND_TABLE, "feature-after-delayed-delete");
    assertEquals(1, featureCount(SECOND_TABLE, "feature-after-delayed-delete"));
    assertMetadataTable(SECOND_TABLE);
  }

  @Test
  public void vizSamplingUsesTheCurrentPhysicalTableAfterSpaceRecreation() throws Exception {
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
    assertNotNull(response);
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
    assertNotNull(deserializeResponse(invokeLambda(event)));
  }

  private static long featureCount(String tableName, String featureId) throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(
            "SELECT count(*) FROM public.\"" + tableName + "\" WHERE id = '" + featureId + "'")) {
      assertTrue(result.next());
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
      assertTrue(result.next());
      return result.getBoolean(1);
    }
  }

  private static void assertMetadataTable(String expectedTableName) throws Exception {
    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        Statement statement = connection.createStatement();
        ResultSet metadata = statement.executeQuery(
            "SELECT h_id FROM xyz_config.space_meta WHERE id = 'recreated-space' AND schem = 'public'")) {
      assertTrue(metadata.next());
      assertEquals(expectedTableName, metadata.getString("h_id"));
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