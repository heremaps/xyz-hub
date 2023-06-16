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

import com.here.xyz.util.json.JsonSerializable;
import com.here.xyz.events.feature.ModifyFeaturesEvent;
import com.here.xyz.events.space.ModifySpaceEvent;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.*;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.xyz.models.hub.Space;

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

import static org.junit.jupiter.api.Assertions.*;

public class PSQLHistoryFullIT extends PSQLAbstractIT {

  static Map<String, Object> connectorParams = new HashMap<String, Object>() {
    {
      put(PsqlHandlerParams.COMPACT_HISTORY, false);
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
  public void testFullHistoryTableWriting() throws Exception {
    // =========== CREATE SPACE with UUID support ==========
    final Space space = new Space("foo");
    //space.setEnableHistory(true);
    final ModifySpaceEvent event = new ModifySpaceEvent();
    //event.setSpaceId("foo");
    event.setOperation(ModifySpaceEvent.Operation.CREATE);
    //event.setConnectorParams(connectorParams);
    event.setSpaceDefinition(space);
    invokeLambda(event.serialize());

    // ============= INSERT ======================
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);
    FeatureCollection collection = new FeatureCollection();
    List<Feature> featureList = new ArrayList<>();

    Point point = new Point().withCoordinates(new PointCoordinates(50, 8));
    Feature f = new Feature("1234");
    f.setGeometry(point);
    f.getProperties().put("foo", 0);
    f.getProperties().setXyzNamespace(xyzNamespace);
    featureList.add(f);
    collection.setLazyParsableFeatureList(featureList);

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
    //mfevent.setConnectorParams(connectorParams);
    //mfevent.setSpaceId("foo");
    mfevent.setTransaction(true);
    mfevent.setEnableUUID(true);

    mfevent.setEnableUUID(true);
    setPUUID(collection);
    mfevent.setInsertFeatures(collection.getFeatures());
    invokeLambda(mfevent.serialize());

    // ============= UPDATE FEATURE 10 Times ======================
    mfevent.setInsertFeatures(null);
    for (int i = 1; i <= 5; i++) {
      f.getProperties().put("foo", i);
      mfevent.setUpdateFeatures(collection.getFeatures());
      setPUUID(collection);
      invokeLambda(mfevent.serialize());
    }

    // ============= DELETE ======================
    mfevent.setDeleteFeatures(new HashMap<String, String>() {{
      put("1234", null);
    }});
    mfevent.setUpdateFeatures(null);
    setPUUID(collection);
    invokeLambda(mfevent.serialize());

    try (final Connection connection = dataSource().getConnection()) {
      Statement stmt = connection.createStatement();
      String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'foo', jsondata->'properties'->'@ns:com:here:xyz'->'deleted' desc";

      ResultSet resultSet = stmt.executeQuery(sql);
      int oldestFooValue = 0;
      int rowCount = 0;

      // Check if 5 last versions are available in history table
      while (resultSet.next()) {
        Feature feature = JsonSerializable.deserialize(resultSet.getString("jsondata"));

        rowCount++;
        if (rowCount == 6) {
          // Here we will find the deleted object which has the foo=5 from the last version.
          assertEquals(oldestFooValue, (int) feature.getProperties().get("foo"));
        } else {
          assertEquals(oldestFooValue++, (int) feature.getProperties().get("foo"));
        }
      }
      // Check if history table has 7 entries (1x insert +5x update +1x delete)
      assertEquals(7, rowCount);
    }
  }

  @Test
  public void testFullHistoryTableTrigger() throws Exception {
    int maxVersionCount = -1;
    // =========== CREATE SPACE with UUID support ==========
    final Space space = new Space("foo");
    //space.setEnableHistory(true);

    final ModifySpaceEvent mse = new ModifySpaceEvent();
    //mse.setSpaceId("foo");
    mse.setOperation(ModifySpaceEvent.Operation.CREATE);
    //mse.setConnectorParams(connectorParams);
    mse.setSpaceDefinition(space);

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
        assertTrue(resultSet.getString("trigger_def").contains("xyz_trigger_historywriter_full('" + maxVersionCount + "')"));
      }
    }
  }
}
