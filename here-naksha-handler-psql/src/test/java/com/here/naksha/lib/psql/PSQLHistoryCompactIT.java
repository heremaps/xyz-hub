/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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

import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.handler.psql.PsqlHandlerParams;
import com.here.naksha.lib.core.models.geojson.coordinates.PointCoordinates;
import com.here.naksha.lib.core.models.geojson.implementation.*;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.naksha.lib.core.models.payload.events.space.ModifySpaceEvent;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class PSQLHistoryCompactIT extends PSQLAbstractIT {

  static Map<String, Object> connectorParams = new HashMap<String, Object>() {
    {
      put(PsqlHandlerParams.COMPACT_HISTORY, true);
      put(PsqlHandlerParams.PROPERTY_SEARCH, true);
    }
  };

  @BeforeAll
  public static void init() throws Exception {
    initEnv(connectorParams);
  }

  @AfterAll
  public void shutdown() throws Exception {
    invokeDeleteTestSpace(connectorParams);
  }

  @Test
  public void testHistoryTableCreation() throws Exception {
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent();
    // mse.setSpaceId("foo");
    mse.setOperation(ModifySpaceEvent.Operation.CREATE);
    // mse.setConnectorParams(connectorParams);
    mse.setSpaceDefinition(new Space("foo"));
    // mse.setEnableHistory(true);

    invokeLambda(mse.serialize());

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT pg_get_triggerdef(oid),"
          + "(SELECT (to_regclass('\"foo\"') IS NOT NULL) as hst_table_exists) "
          + "FROM pg_trigger "
          + "WHERE tgname = 'TR_foo_HISTORY_WRITER';";

      ResultSet resultSet = stmt.executeQuery(sql);
      if (!resultSet.next()) {
        throw new Exception("History Trigger/Table is missing!");
      } else {
        assertTrue(resultSet.getBoolean("hst_table_exists"));
      }
    }
  }

  @Test
  public void testHistoryTableWriting() throws Exception {
    int maxVersionCount = 5;
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent();
    // mse.setSpaceId("foo");
    mse.setOperation(ModifySpaceEvent.Operation.CREATE);
    // mse.setConnectorParams(connectorParams);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)

    invokeLambda(mse.serialize());

    // ============= INSERT ======================
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    XyzFeatureCollection collection = new XyzFeatureCollection();
    List<XyzFeature> featureList = new ArrayList<>();

    XyzPoint point = new XyzPoint().withCoordinates(new PointCoordinates(50, 8));
    XyzFeature f = new XyzFeature("1234");
    f.setGeometry(point);
    f.getProperties().put("foo", 0);
    featureList.add(f);
    collection.setLazyParsableFeatureList(featureList);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    // mfevent.setConnectorParams(connectorParams);
    // mfevent.setSpaceId("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);

    setPUUID(collection);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    // ============= UPDATE FEATURE 10 Times ======================
    mfevent.setInsertFeatures(null);
    for (int i = 1; i <= 10; i++) {
      f.getProperties().put("foo", i);
      mfevent.setUpdateFeatures(collection.getFeatures());
      setPUUID(collection);
      invokeLambda(mfevent.serialize());
    }

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'foo'";

      ResultSet resultSet = stmt.executeQuery(sql);
      int oldestFooValue = 5;
      int rowCount = 0;

      // Check if 5 last versions are available in history table
      while (resultSet.next()) {
        XyzFeature feature = JsonSerializable.deserialize(resultSet.getString("jsondata"));
        assertEquals(oldestFooValue++, (int) feature.getProperties().get("foo"));
        rowCount++;
      }
      // Check if history table has only 5 entries
      assertEquals(5, rowCount);
    }

    // set history to infinite
    mse = new ModifySpaceEvent();
    // mse.setSpaceId("foo");
    // mse.setConnectorParams(connectorParams);
    mse.setOperation(ModifySpaceEvent.Operation.UPDATE);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)
    invokeLambda(mse.serialize());

    // ============= UPDATE FEATURE 10 Times ======================
    for (int i = 11; i <= 20; i++) {
      f.getProperties().put("foo", i); // (new Properties().with("foo", i).withXyzNamespace(xyzNamespace));
      mfevent.setUpdateFeatures(collection.getFeatures());
      setPUUID(collection);
      invokeLambda(mfevent.serialize());
    }

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'foo'";

      ResultSet resultSet = stmt.executeQuery(sql);
      // Oldes history item has foo=9
      int oldestFooValue = 5;
      int rowCount = 0;

      // Check if all versions are available
      while (resultSet.next()) {
        XyzFeature feature = JsonSerializable.deserialize(resultSet.getString("jsondata"));
        assertEquals(oldestFooValue++, (int) feature.getProperties().get("foo"));
        rowCount++;
      }
      // Check if history table has 15 entries
      assertEquals(15, rowCount);
    }

    // set withMaxVersionCount to 2
    mse = new ModifySpaceEvent();
    // mse.setSpaceId("foo");
    // mse.setConnectorParams(connectorParams);
    mse.setOperation(ModifySpaceEvent.Operation.UPDATE);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)
    invokeLambda(mse.serialize());

    // Do one Update to fire the updated trigger
    f.getProperties().put("foo", 21);
    mfevent.setUpdateFeatures(collection.getFeatures());
    setPUUID(collection);
    invokeLambda(mfevent.serialize());

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'foo'";

      ResultSet resultSet = stmt.executeQuery(sql);
      // Oldest history item has foo=19 - all other should be deleted related to maxVersionCount=2
      // update
      int oldestFooValue = 19;
      int rowCount = 0;

      // Check if only two versions are left in the history
      while (resultSet.next()) {
        XyzFeature feature = JsonSerializable.deserialize(resultSet.getString("jsondata"));
        assertEquals(oldestFooValue++, (int) feature.getProperties().get("foo"));
        rowCount++;
      }
      // Check if history table has 2 entries
      assertEquals(2, rowCount);
    }
  }

  @Test
  public void testHistoryTableDeletedFlag() throws Exception {
    int maxVersionCount = 5;
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent();
    // mse.setSpaceId("foo");
    // mse.setParams(Collections.singletonMap("maxVersionCount", maxVersionCount));
    mse.setOperation(ModifySpaceEvent.Operation.CREATE);
    // mse.setConnectorParams(connectorParams);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)

    String response = invokeLambda(mse.serialize());

    // ============= INSERT ======================
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    XyzFeatureCollection collection = new XyzFeatureCollection();
    List<XyzFeature> featureList = new ArrayList<>();

    XyzPoint point = new XyzPoint().withCoordinates(new PointCoordinates(50, 8));
    XyzFeature f = new XyzFeature("1234");
    f.setGeometry(point);
    f.getProperties().put("foo", 0);
    featureList.add(f);
    collection.setLazyParsableFeatureList(featureList);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    // mfevent.setConnectorParams(connectorParams);
    // mfevent.setSpaceId("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);

    setPUUID(collection);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    // DELETE feature
    mfevent.setUpdateFeatures(null);
    mfevent.setDeleteFeatures(new HashMap<String, String>() {
      {
        put("1234", null);
      }
    });
    invokeLambda(mfevent.serialize());

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql =
          "SELECT * from foo_hst ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt' DESC LIMIT 1;";

      ResultSet resultSet = stmt.executeQuery(sql);
      resultSet.next();
      XyzFeature feature = JsonSerializable.deserialize(resultSet.getString("jsondata"));
      assertTrue(feature.getProperties().getXyzNamespace().isDeleted());
    }
  }

  @Test
  public void testHistoryTableTrigger() throws Exception {
    int maxVersionCount = 8;
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent();
    // mse.setSpaceId("foo");
    mse.setOperation(ModifySpaceEvent.Operation.CREATE);
    // mse.setConnectorParams(connectorParams);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)

    invokeLambda(mse.serialize());

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT pg_get_triggerdef(oid) as trigger_def "
          + "FROM pg_trigger "
          + "WHERE tgname = 'TR_foo_HISTORY_WRITER';";

      ResultSet resultSet = stmt.executeQuery(sql);
      if (!resultSet.next()) {
        throw new Exception("History Trigger/Table is missing!");
      } else {
        assertTrue(resultSet
            .getString("trigger_def")
            .contains("xyz_trigger_historywriter('" + maxVersionCount + "')"));
      }
    }
  }
}
