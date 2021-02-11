/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.*;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.config.ConnectorParameters;
import org.junit.After;
import org.junit.Before;
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

public class PSQLHistoryFullIT extends PSQLAbstractIT {

    static Map<String, Object> connectorParams = new HashMap<String,Object>(){
        {put(ConnectorParameters.CONNECTOR_ID, "test-connector");put(ConnectorParameters.COMPACT_HISTORY, false);put(ConnectorParameters.PROPERTY_SEARCH, true);}};

    @BeforeClass
    public static void init() throws Exception { initEnv(connectorParams); }

    @Before
    public void removeTestSpaces() throws Exception { deleteTestSpace(connectorParams); }

    @After
    public void shutdown() throws Exception { shutdownEnv(connectorParams); }

    @Test
    public void testFullHistoryTableWriting() throws Exception {
        // =========== CREATE SPACE with UUID support ==========
        ModifySpaceEvent mse = new ModifySpaceEvent()
                .withSpace("foo")
                .withOperation(ModifySpaceEvent.Operation.CREATE)
                .withConnectorParams(connectorParams)
                .withSpaceDefinition(new Space()
                        .withId("foo")
                        .withEnableUUID(true)
                        .withEnableHistory(true)
                );

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

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(connectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withEnableUUID(true);

        mfevent.setEnableUUID(true);
        setPUUID(collection);
        mfevent.setInsertFeatures(collection.getFeatures());
        invokeLambda(mfevent.serialize());

        // ============= UPDATE FEATURE 10 Times ======================
        mfevent.setInsertFeatures(null);
        for (int i = 1; i <= 5; i++) {
            f.getProperties().with("foo", i);
            mfevent.setUpdateFeatures(collection.getFeatures());
            setPUUID(collection);
            invokeLambda(mfevent.serialize());
        }

        // ============= DELETE ======================
        mfevent.setDeleteFeatures(new HashMap<String,String>(){{put("1234",null);}});
        mfevent.setUpdateFeatures(null);
        setPUUID(collection);
        invokeLambda(mfevent.serialize());

        try (final Connection connection = lambda.dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            String sql = "SELECT * from foo_hst ORDER BY jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt' DESC;";

            ResultSet resultSet = stmt.executeQuery(sql);
            int oldestFooValue = 0;
            int rowCount = 0;

            // Check if 5 last versions are available in history table
            while (resultSet.next()) {
                Feature feature = XyzSerializable.deserialize(resultSet.getString("jsondata"));

                rowCount++;
                if(rowCount == 6){
                    // Here we will find the deleted object which has the foo=5 from the last version.
                    assertEquals(oldestFooValue, (int) feature.getProperties().get("foo"));
                }
                else
                    assertEquals(oldestFooValue++, (int) feature.getProperties().get("foo"));
            }
            // Check if history table has 7 entries (1x insert +5x update +1x delete)
            assertEquals(7, rowCount);
        }
    }

    @Test
    public void testFullHistoryTableTrigger() throws Exception {
        int maxVersionCount = -1;
        // =========== CREATE SPACE with UUID support ==========
        ModifySpaceEvent mse = new ModifySpaceEvent()
                .withSpace("foo")
                .withOperation(ModifySpaceEvent.Operation.CREATE)
                .withConnectorParams(connectorParams)
                .withSpaceDefinition(new Space()
                        .withId("foo")
                        .withEnableUUID(true)
                        .withEnableHistory(true)
                        .withMaxVersionCount(maxVersionCount)
                );

        invokeLambda(mse.serialize());

        try (final Connection connection = lambda.dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            String sql = "SELECT pg_get_triggerdef(oid) as trigger_def " +
                    "FROM pg_trigger " +
                    "WHERE tgname = 'TR_foo_HISTORY_WRITER';";

            ResultSet resultSet = stmt.executeQuery(sql);
            if(!resultSet.next()) {
                throw new Exception("History Trigger/Table is missing!");
            }else{
                assertTrue(resultSet.getString("trigger_def").contains("xyz_trigger_historywriter_full('"+maxVersionCount+"')"));
            }
        }
    }
}
