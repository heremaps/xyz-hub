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
package com.here.xyz.psql;

import com.here.xyz.models.hub.psql.PsqlStorageParams;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.*;
import com.here.xyz.models.hub.Space;
import java.util.Collections;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PSQLHistoryCompactIT extends PSQLAbstractIT {

  static Map<String, Object> connectorParams = new HashMap<String, Object>() {
    {
      put(PsqlStorageParams.ID, "test-connector");
      put(PsqlStorageParams.COMPACT_HISTORY, true);
      put(PsqlStorageParams.PROPERTY_SEARCH, true);
    }
  };

  @BeforeClass
  public static void init() throws Exception {
    initEnv(connectorParams);
  }

  @After
  public void shutdown() throws Exception {
    invokeDeleteTestSpace(connectorParams);
  }

  @Test
  public void testHistoryTableCreation() throws Exception {
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent();
    mse.setSpace("foo");
    mse.setOperation(ModifySpaceEvent.Operation.CREATE);
    mse.setConnectorParams(connectorParams);
    mse.setSpaceDefinition(new Space("foo"));
    //mse.setEnableHistory(true);

    invokeLambda(mse.serialize());

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT pg_get_triggerdef(oid)," +
          "(SELECT (to_regclass('\"foo\"') IS NOT NULL) as hst_table_exists) " +
          "FROM pg_trigger " +
          "WHERE tgname = 'TR_foo_HISTORY_WRITER';";

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
    mse.setSpace("foo");
    mse.setOperation(ModifySpaceEvent.Operation.CREATE);
    mse.setConnectorParams(connectorParams);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)

    invokeLambda(mse.serialize());

    // ============= INSERT ======================
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    FeatureCollection collection = new FeatureCollection();
    List<Feature> featureList = new ArrayList<>();

    Point point = new Point().withCoordinates(new PointCoordinates(50, 8));
    Feature f = new Feature()
        .withId("1234")
        .withGeometry(point)
        .withProperties(new Properties().with("foo", 0).withXyzNamespace(xyzNamespace));
    featureList.add(f);
    collection.setFeatures(featureList);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setConnectorParams(connectorParams);
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);

    setPUUID(collection);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    // ============= UPDATE FEATURE 10 Times ======================
    mfevent.setInsertFeatures(null);
    for (int i = 1; i <= 10; i++) {
      f.getProperties().with("foo", i);
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
        Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));
        assertEquals(oldestFooValue++, (int) feature.getProperties().get("foo"));
        rowCount++;
      }
      // Check if history table has only 5 entries
      assertEquals(5, rowCount);
    }

    // set history to infinite
    mse = new ModifySpaceEvent();
    mse.setSpace("foo");
    mse.setConnectorParams(connectorParams);
    mse.setOperation(ModifySpaceEvent.Operation.UPDATE);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)
    invokeLambda(mse.serialize());

    // ============= UPDATE FEATURE 10 Times ======================
    for (int i = 11; i <= 20; i++) {
      f.getProperties().with("foo", i);//(new Properties().with("foo", i).withXyzNamespace(xyzNamespace));
      mfevent.setUpdateFeatures(collection.getFeatures());
      setPUUID(collection);
      invokeLambda(mfevent.serialize());
    }

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'foo'";

      ResultSet resultSet = stmt.executeQuery(sql);
      //Oldes history item has foo=9
      int oldestFooValue = 5;
      int rowCount = 0;

      // Check if all versions are available
      while (resultSet.next()) {
        Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));
        assertEquals(oldestFooValue++, (int) feature.getProperties().get("foo"));
        rowCount++;
      }
      // Check if history table has 15 entries
      assertEquals(15, rowCount);
    }

    // set withMaxVersionCount to 2
    mse = new ModifySpaceEvent();
    mse.setSpace("foo");
    mse.setConnectorParams(connectorParams);
    mse.setOperation(ModifySpaceEvent.Operation.UPDATE);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)
    invokeLambda(mse.serialize());

    //Do one Update to fire the updated trigger
    f.getProperties().with("foo", 21);
    mfevent.setUpdateFeatures(collection.getFeatures());
    setPUUID(collection);
    invokeLambda(mfevent.serialize());

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'foo'";

      ResultSet resultSet = stmt.executeQuery(sql);
      //Oldest history item has foo=19 - all other should be deleted related to maxVersionCount=2 update
      int oldestFooValue = 19;
      int rowCount = 0;

      // Check if only two versions are left in the history
      while (resultSet.next()) {
        Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));
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
        mse.setSpace("foo");
    mse.setOperation(ModifySpaceEvent.Operation.CREATE);
    mse.setParams(Collections.singletonMap("maxVersionCount", maxVersionCount));
    mse.setConnectorParams(connectorParams);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)

    String response = invokeLambda(mse.serialize());

    // ============= INSERT ======================
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    FeatureCollection collection = new FeatureCollection();
    List<Feature> featureList = new ArrayList<>();

    Point point = new Point().withCoordinates(new PointCoordinates(50, 8));
    Feature f = new Feature()
        .withId("1234")
        .withGeometry(point)
        .withProperties(new Properties().with("foo", 0).withXyzNamespace(xyzNamespace));
    featureList.add(f);
    collection.setFeatures(featureList);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    mfevent.setConnectorParams(connectorParams);
    mfevent.setSpace("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);

    setPUUID(collection);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    //DELETE feature
    mfevent.setUpdateFeatures(null);
    mfevent.setDeleteFeatures(new HashMap<String, String>() {{
      put("1234", null);
    }});
    invokeLambda(mfevent.serialize());

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt' DESC LIMIT 1;";

      ResultSet resultSet = stmt.executeQuery(sql);
      resultSet.next();
      Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));
      assertTrue(feature.getProperties().getXyzNamespace().isDeleted());
    }
  }

  @Test
  public void testHistoryTableTrigger() throws Exception {
    int maxVersionCount = 8;
    // =========== CREATE SPACE with UUID support ==========
    ModifySpaceEvent mse = new ModifySpaceEvent();
    mse.setSpace("foo");
    mse.setOperation(ModifySpaceEvent.Operation.CREATE);
    mse.setConnectorParams(connectorParams);
    mse.setSpaceDefinition(new Space("foo")); // .withEnableHistory(true)

    invokeLambda(mse.serialize());

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT pg_get_triggerdef(oid) as trigger_def " +
          "FROM pg_trigger " +
          "WHERE tgname = 'TR_foo_HISTORY_WRITER';";

      ResultSet resultSet = stmt.executeQuery(sql);
      if (!resultSet.next()) {
        throw new Exception("History Trigger/Table is missing!");
      } else {
        assertTrue(resultSet.getString("trigger_def").contains("xyz_trigger_historywriter('" + maxVersionCount + "')"));
      }
    }
  }
}
