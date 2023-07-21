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
package com.here.xyz.psql;

import static com.here.xyz.events.ModifySpaceEvent.Operation.CREATE;
import static com.here.xyz.events.ModifySpaceEvent.Operation.UPDATE;
import static com.here.xyz.psql.query.ModifySpace.IDX_STATUS_TABLE_FQN;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.config.ConnectorParameters;
import com.here.xyz.psql.tools.DhString;
import com.here.xyz.psql.tools.FeatureGenerator;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.SuccessResponse;
import com.here.xyz.responses.XyzError;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PSQLIndexIT extends PSQLAbstractIT {

    static Map<String, Object> connectorParams = new HashMap<String,Object>(){
        {   put(ConnectorParameters.CONNECTOR_ID, "test-connector");
            put(ConnectorParameters.AUTO_INDEXING, true);
            put(ConnectorParameters.PROPERTY_SEARCH, true);
        }
    };

    @Before
    public void createTable() throws Exception {
        invokeCreateTestSpace(connectorParams, TEST_SPACE_ID);
    }

    @BeforeClass
    public static void init() throws Exception { initEnv(connectorParams); }

    @After
    public void shutdown() throws Exception { invokeDeleteTestSpace(connectorParams); }

    @Test
    public void testOnDemandIdxLimit() throws Exception {
        Map<String,Boolean> searchableProperties = new HashMap(){{
            put("foo1",true);
            put("foo2",true);
            put("foo3",true);
            put("foo4",true);
            put("foo5",true);
        }};

        /** Set 5 searchable Properties - 4 should be allowed by default */
        ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent().withSpace("foo")
            .withOperation(CREATE)
            .withConnectorParams(connectorParams)
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withSearchableProperties(searchableProperties)
            );
        ErrorResponse error = XyzSerializable.deserialize(invokeLambda(modifySpaceEvent.serialize()));
        assertEquals(XyzError.ILLEGAL_ARGUMENT, error.getError());
        assertEquals("On-Demand-Indexing - Maximum permissible: 4 searchable properties per space!", error.getErrorMessage());

        searchableProperties.remove("foo5");

        /** Set 4 searchable Properties */
        modifySpaceEvent = new ModifySpaceEvent().withSpace("foo")
            .withOperation(CREATE)
            .withConnectorParams(connectorParams)
            //Table gets created also without features
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withSearchableProperties(searchableProperties)
            );

        SuccessResponse response = deserializeResponse(invokeLambda(modifySpaceEvent.serialize()));
        assertEquals("OK",response.getStatus());

        /** Increase to 5 allowed Indices */
        connectorParams.put(ConnectorParameters.ON_DEMAND_IDX_LIMIT, 5);
        /** deactivated ones does not get into account - result will be 5 which are required */
        searchableProperties.put("foo5",true);
        searchableProperties.put("foo6",false);
        searchableProperties.put("foo7",false);

        modifySpaceEvent = new ModifySpaceEvent().withSpace("foo")
            .withOperation(UPDATE)
            .withConnectorParams(connectorParams)
            .withSpaceDefinition(new Space()
                    .withId("foo")
                    .withSearchableProperties(searchableProperties)
            );

        response = XyzSerializable.deserialize(invokeLambda(modifySpaceEvent.serialize()));
        assertEquals("OK",response.getStatus());

        try (final Connection connection = LAMBDA.dataSource.getConnection()) {
            /** Default System Indices */
            List<String> systemIndices = new ArrayList<String>(){{
                add("createdAt");
                add("updatedAt");
                add("serial");
                add("geo");
                add("tags");
            }};

            String sqlSpaceSchema = "(select schema_name::text from information_schema.schemata where schema_name in ('xyz','public') order by 1 desc limit 1)";

            Statement stmt = connection.createStatement();
            stmt.execute( DhString.format("select xyz_maintain_idxs_for_space( %s, 'foo');",sqlSpaceSchema));

            /** Check which Indices are available */
            ResultSet resultSet = stmt.executeQuery( DhString.format("select idx_name, idx_property, src from xyz_index_list_all_available(%s, 'foo');",sqlSpaceSchema));
            while(resultSet.next()){
                String idxProperty = resultSet.getString("idx_property");
                if(systemIndices.contains(idxProperty))
                    systemIndices.remove(idxProperty);
                else
                    searchableProperties.remove(idxProperty);
            }
            /** If all System Indices could get found the list should be empty */
            assertEquals(0,systemIndices.size());
            /** If foo1:foo5 could get found only foo6 & foo7 should be in the map */
            assertEquals(2,searchableProperties.size());
        }
    }

    @Test
    public void testOnDemandIdxCreation() throws Exception {
        //Create space
        ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent().withSpace("foo")
                .withOperation(CREATE)
                .withConnectorParams(connectorParams)
                //Table gets created also without features
                .withSpaceDefinition(new Space().withId("foo"));

        SuccessResponse response = deserializeResponse(invokeLambda(modifySpaceEvent.serialize()));
        assertEquals("OK",response.getStatus());

        //Update space, add searchable properties
        Map<String,Boolean> searchableProperties = new HashMap(){{
            put("foo1",true);
            put("foo2",true);
            put("foo3",true);
            put("foo4",true);
            put("foo5",false);
            put("foo6",false);
        }};
        modifySpaceEvent = new ModifySpaceEvent().withSpace("foo")
            .withOperation(UPDATE)
            .withConnectorParams(connectorParams)
            //Table gets created also without features
            .withSpaceDefinition(new Space()
                .withId("foo")
                .withSearchableProperties(searchableProperties)
            );

        response = deserializeResponse(invokeLambda(modifySpaceEvent.serialize()));
        assertEquals("OK", response.getStatus());


        try (final Connection connection = LAMBDA.dataSource.getConnection()) {
            /** Default System Indices */
            List<String> systemIndices = new ArrayList<>(Arrays.asList(
                "createdAt",
                "updatedAt",
                "serial",
                "geo",
                "tags",
                "viz",
                "idnew",
                "version",
                "nextversion",
                "idversion",
                "operation",
                "author"
            ));

            String sqlSpaceSchema = "(select schema_name::text from information_schema.schemata where schema_name in ('xyz','public') order by 1 desc limit 1)";

            Statement stmt = connection.createStatement();
            stmt.execute( DhString.format("select xyz_maintain_idxs_for_space( %s, 'foo');",sqlSpaceSchema));

            /** Check which Indices are available */
            ResultSet resultSet = stmt.executeQuery( DhString.format("select idx_property, src from xyz_index_list_all_available(%s, 'foo');",sqlSpaceSchema));
            while(resultSet.next()){
                String idxProperty = resultSet.getString("idx_property");
                if(systemIndices.contains(idxProperty)) {
                    if (!idxProperty.equals("Key"))
                        systemIndices.remove(idxProperty);
                    assertEquals("s",resultSet.getString("src"));
                }
                else {
                    searchableProperties.remove(idxProperty);
                    assertEquals("m",resultSet.getString("src"));
                }

            }
            /** If all System Indices could get found the list should be empty */
            assertEquals(Collections.emptyList(), systemIndices);
            /** If foo1:foo5 could get found only foo6 & foo7 should be in the map */
            assertEquals(2,searchableProperties.size());
        }

        GetStatisticsEvent statisticsEvent = new GetStatisticsEvent()
                .withSpace("foo")
                .withConnectorParams(connectorParams);
        // =========== Invoke GetStatisticsEvent ==========
        StatisticsResponse resp = XyzSerializable.deserialize(invokeLambda(statisticsEvent.serialize()));
        assertEquals(StatisticsResponse.PropertiesStatistics.Searchable.ALL, resp.getProperties().getSearchable());

        List<StatisticsResponse.PropertyStatistics> propStatistics = resp.getProperties().getValue();
        for (StatisticsResponse.PropertyStatistics propStat: propStatistics ) {
           assertEquals("foo",propStat.getKey().substring(0,3));
           /**TODO: Clarify Behavior (StatisticsResponse) */
           //assertNull(propStat.isSearchable());
        }
    }

    @Test
    public void testOnDemandIndexContent() throws Exception {
        //Create space
        ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent().withSpace("foo")
            .withOperation(CREATE)
            .withConnectorParams(connectorParams)
            //Table gets created also without features
            .withSpaceDefinition(new Space().withId("foo"));

        SuccessResponse response = deserializeResponse(invokeLambda(modifySpaceEvent.serialize()));
        assertEquals("OK",response.getStatus());

        //Update space, add searchable properties
        Map<String,Boolean> searchableProperties = new HashMap(){{
            put("foo",true);
            put("foo2::array",true);
            put("foo.nested",true);
            put("f.fooroot",true);
            put("f.geometry.type",true);
        }};

        /** Increase to 5 allowed Indices */
        connectorParams.put(ConnectorParameters.ON_DEMAND_IDX_LIMIT, 5);

        modifySpaceEvent = new ModifySpaceEvent().withSpace("foo")
                .withOperation(UPDATE)
                .withConnectorParams(connectorParams)
                //Table gets created also without features
                .withSpaceDefinition(new Space()
                        .withId("foo")
                        .withSearchableProperties(searchableProperties)
                );

        response = deserializeResponse(invokeLambda(modifySpaceEvent.serialize()));
        assertEquals("OK",response.getStatus());

        try (final Connection connection = LAMBDA.dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("select xyz_maintain_idxs_for_space('public', 'foo');");

            ResultSet resultSet = stmt.executeQuery("SELECT idx_name, idx_property, \n" +
                    " (SELECT indexdef FROM pg_indexes WHERE tablename = 'foo' AND indexname=idx_name)\n" +
                    " FROM xyz_index_list_all_available('public', 'foo')");

            while(resultSet.next()) {
                String idx_property = resultSet.getString("idx_property");
                String idx_name = resultSet.getString("idx_name");
                String indexdef = resultSet.getString("indexdef");

                switch (idx_property){
                    case "foo" :
                        assertEquals("CREATE INDEX "+idx_name+" ON ONLY public.foo USING btree ((((jsondata -> 'properties'::text) -> 'foo'::text)))",indexdef);
                        break;
                    case "foo2" :
                        assertEquals("CREATE INDEX "+idx_name+" ON ONLY public.foo USING gin ((((jsondata -> 'properties'::text) -> 'foo2'::text)))",indexdef);
                        break;
                    case "foo.nested" :
                        assertEquals("CREATE INDEX "+idx_name+" ON ONLY public.foo USING btree (((((jsondata -> 'properties'::text) -> 'foo'::text) -> 'nested'::text)))",indexdef);
                        break;
                    case "f.fooroot" :
                        assertEquals("CREATE INDEX "+idx_name+" ON ONLY public.foo USING btree (((jsondata -> 'fooroot'::text)))",indexdef);
                        break;
                    case "f.geometry.type" :
                        assertEquals("CREATE INDEX "+idx_name+" ON ONLY public.foo USING btree (geometrytype(geo))",indexdef);
                        break;
                    case "id" :
                        assertEquals("CREATE INDEX idx_foo_id ON ONLY public.foo USING btree (((jsondata ->> 'id'::text)))",indexdef);
                        break;
                    case "geo" :
                        assertEquals("CREATE INDEX idx_foo_geo ON ONLY public.foo USING gist (geo)",indexdef);
                        break;
                    case "serial" :
                        assertEquals("CREATE INDEX idx_foo_serial ON ONLY public.foo USING btree (i)",indexdef);
                        break;
                    case "tags" :
                        assertEquals("CREATE INDEX idx_foo_tags ON ONLY public.foo USING gin (((((jsondata -> 'properties'::text) -> '@ns:com:here:xyz'::text) -> 'tags'::text)))",indexdef);
                        break;
                    case "createdAt" :
                        assertEquals("CREATE INDEX \"idx_foo_createdAt\" ON ONLY public.foo USING btree (((((jsondata -> 'properties'::text) -> '@ns:com:here:xyz'::text) -> 'createdAt'::text)), id)",indexdef);
                        break;
                    case "updatedAt" :
                        assertEquals("CREATE INDEX \"idx_foo_updatedAt\" ON ONLY public.foo USING btree (((((jsondata -> 'properties'::text) -> '@ns:com:here:xyz'::text) -> 'updatedAt'::text)), id)",indexdef);
                        break;
                }
            }
        }
    }

    @Test
    public void testAutoIndexing() throws Exception {
        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withSpace("foo")
                .withTransaction(true)
                .withInsertFeatures(FeatureGenerator.get11kFeatureCollection().getFeatures())
                .withConnectorParams(connectorParams);

        invokeLambda(mfevent.serialize());

        /** Needed to trigger update on pg_stat */
        try (final Connection connection = LAMBDA.dataSource.getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("DELETE FROM " + IDX_STATUS_TABLE_FQN + " WHERE spaceid='foo';");
            stmt.execute("ANALYZE \"foo\";");
        }

        // =========== Invoke HealthCheck - Triggers dbMaintenance (with index-creation) ==========
        invokeLambdaFromFile("/events/HealthCheckEventWithAutoIndexing.json");

        GetStatisticsEvent statisticsEvent = new GetStatisticsEvent()
                .withSpace("foo")
                .withConnectorParams(connectorParams);
        // =========== Invoke GetStatisticsEvent ==========
        String stringResponse = invokeLambda(statisticsEvent.serialize());
        StatisticsResponse response = deserializeResponse(stringResponse);

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
                .withOperation(UPDATE)
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
