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
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.config.ConnectorParameters;
import com.here.xyz.responses.StatisticsResponse;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class PSQLIndexIT extends PSQLAbstractIT {

    static Map<String, Object> connectorParams = new HashMap<String,Object>(){
        {put(ConnectorParameters.CONNECTOR_ID, "test-connector");put(ConnectorParameters.AUTO_INDEXING, true);put(ConnectorParameters.PROPERTY_SEARCH, true);}};

    @BeforeClass
    public static void init() throws Exception { initEnv(connectorParams); }

    @Before
    public void removeTestSpaces() throws Exception { deleteTestSpace(connectorParams); }

    @After
    public void shutdown() throws Exception { shutdownEnv(connectorParams); }

    @Test
    public void testAutoIndexing() throws Exception {
        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withSpace("foo")
                .withTransaction(true)
                .withInsertFeatures(get11kFeatureCollection().getFeatures())
                .withConnectorParams(connectorParams);

        invokeLambda(mfevent.serialize());

        /** Needed to trigger update on pg_stat */
        try (final Connection connection = lambda.dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("DELETE FROM xyz_config.xyz_idxs_status WHERE spaceid='foo';");
            stmt.execute("ANALYZE \"foo\";");
        }

        // =========== Invoke HealthCheck - Triggers dbMaintenance (with index-creation) ==========
        invokeLambdaFromFile("/events/HealthCheckEventWithAutoIndexing.json");

        GetStatisticsEvent statisticsEvent = new GetStatisticsEvent()
                .withSpace("foo")
                .withConnectorParams(connectorParams);
        // =========== Invoke GetStatisticsEvent ==========
        String stringResponse = invokeLambda(statisticsEvent.serialize());
        StatisticsResponse response = XyzSerializable.deserialize(stringResponse);

        assertNotNull(response);
        assertEquals(new Long(11000), response.getCount().getValue());
        assertEquals(true,  response.getCount().getEstimated());
        assertEquals(StatisticsResponse.PropertiesStatistics.Searchable.PARTIAL, response.getProperties().getSearchable());

        List<StatisticsResponse.PropertyStatistics> propStatistics = response.getProperties().getValue();
        for (StatisticsResponse.PropertyStatistics propStat: propStatistics ) {
            if(propStat.getKey().equalsIgnoreCase("test")){
                /** The value test should not get indexed because it has only one value */
                assertEquals("number", propStat.getDatatype());
                assertEquals(false, propStat.isSearchable());
                assertTrue(propStat.getCount() < 11000);
            }else{
                /** All other values should get indexed */
                assertEquals("string", propStat.getDatatype());
                assertEquals(true, propStat.isSearchable());
                assertEquals(11000 , propStat.getCount());
            }
        }

        /** Deactivate autoIndexing */
        ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent().withSpace("foo")
                .withOperation(ModifySpaceEvent.Operation.UPDATE)
                .withConnectorParams(connectorParams)
                .withSpaceDefinition(new Space()
                        .withId("foo")
                        .withEnableAutoSearchableProperties(false)
                );

        // =========== Invoke ModifySpaceEvent ==========
        invokeLambda(modifySpaceEvent.serialize());

        // =========== Invoke HealthCheck - Triggers dbMaintenance (with index-deletion) ==========
        invokeLambdaFromFile("/events/HealthCheckEventWithAutoIndexing.json");

        stringResponse = invokeLambda(statisticsEvent.serialize());
        response = XyzSerializable.deserialize(stringResponse);
        assertNotNull(response);

        propStatistics = response.getProperties().getValue();
        for (StatisticsResponse.PropertyStatistics propStat: propStatistics ) {
            /** No Auto-Indices should be present anymore */
            if(propStat.getKey().equalsIgnoreCase("test")){
                assertEquals("number", propStat.getDatatype());
                assertEquals(false, propStat.isSearchable());
            }else{
                assertEquals("string", propStat.getDatatype());
                assertEquals(false, propStat.isSearchable());
            }
        }
    }
}
