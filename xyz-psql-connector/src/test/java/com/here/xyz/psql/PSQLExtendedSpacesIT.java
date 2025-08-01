/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.XyzSerializable.Mappers.DEFAULT_MAPPER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.models.hub.Space;
import com.here.xyz.psql.query.ModifySpace;
import com.here.xyz.psql.tools.FeatureGenerator;
import com.here.xyz.responses.SuccessResponse;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class PSQLExtendedSpacesIT extends PSQLAbstractIT {
    private static final String BASE1 = "base1_test";
    private static final String BASE2 = "base2_test";
    private static final String DELTA1 = "delta1_test";
    private static final String DELTA2 = "delta2_test";

    private static List<String> spaces = new ArrayList<String>(){{add(BASE1);add(BASE2);add(DELTA1);add(DELTA2);}};

    protected static Map<String, Object> connectorParams = new HashMap<String,Object>(){
        {   put(PSQLAbstractIT.CONNECTOR_ID, "test-connector");
            put(PSQLAbstractIT.AUTO_INDEXING, true);
            put(PSQLAbstractIT.PROPERTY_SEARCH, true);
        }
    };

    @BeforeClass
    public static void init() throws Exception {
        initEnv(connectorParams);
        generateTestSpaces();
    }

    @After
    public void shutdown() throws Exception { invokeDeleteTestSpaces(connectorParams, spaces); }

    @Test
    public void checkIDX() throws Exception {
        checkIDXTable(1, false);

        Map<String,Boolean> searchableProperties = new HashMap();
        List<List<Object>> sortableProperties = new ArrayList<>();
        searchableProperties.put("search_test", false);
        searchableProperties.put("search_test2", true);

        /** Update Searchable and SortableProperties in Base */
        ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent()
            .withSpace(BASE1)
            .withOperation(ModifySpaceEvent.Operation.UPDATE)
            .withConnectorParams(connectorParams)
            .withSpaceDefinition(
                    new Space()
                            .withId(BASE1)
                            .withSearchableProperties(searchableProperties)
                            .withSortableProperties(sortableProperties)
            );
        SuccessResponse response = XyzSerializable.deserialize(invokeLambda(modifySpaceEvent));
        assertEquals("OK",response.getStatus());

        /** Check if IDX-Table reflects this changes */
        checkIDXTable(2, false);

        /** Change Base Layer */
        Map<String, Object> params = new HashMap<>();
        params.put("extends",new Space.Extension().withSpaceId(BASE2));

        modifySpaceEvent = new ModifySpaceEvent()
                .withSpace(DELTA1)
                .withOperation(ModifySpaceEvent.Operation.UPDATE)
                .withConnectorParams(connectorParams)
                .withParams(params)
                .withSpaceDefinition(
                        new Space()
                                .withId(DELTA1)
                );
        response = XyzSerializable.deserialize(invokeLambda(modifySpaceEvent));
        assertEquals("OK",response.getStatus());

        /** Check if IDX-Table reflects this changes */
        checkIDXTable(3, true);
    }

    protected static void generateTestSpaces() throws Exception {
        /** Generate:
         * BASE
         * DELTA1 Extends BASE
         * DELTA2 Extends DELTA1
         * */
        for (String space : spaces ) {
            Map<String,Boolean> searchableProperties = new HashMap();
            List<List<Object>> sortableProperties = new ArrayList<>();
            Map<String, Object> params = new HashMap<>();
            Map<String, Object> extendsL2 = new HashMap<>();

            switch (space){
                case BASE1:
                    searchableProperties.put("search_test", true);
                    sortableProperties.add(new ArrayList<Object>(){{add("sort_test");}});
                    break;
                case BASE2:
                    searchableProperties.put("search_test_base2", true);
                    break;
                case DELTA1:
                    mockAutoIndexing();
                    params.put("extends",new Space.Extension().withSpaceId(BASE1));
                    break;
                case DELTA2:
                    extendsL2.put("spaceId",DELTA1);
                    extendsL2.put("extends",new Space.Extension().withSpaceId(BASE1));
                    params.put("extends",extendsL2);
            }

            ModifySpaceEvent modifySpaceEvent = new ModifySpaceEvent()
                    .withSpace(space)
                    .withOperation(ModifySpaceEvent.Operation.CREATE)
                    .withConnectorParams(connectorParams)
                    .withParams(params)
                    .withSpaceDefinition(
                            new Space()
                                .withId(space)
                                .withSearchableProperties(searchableProperties)
                                .withSortableProperties(sortableProperties)
                    );
            SuccessResponse response = XyzSerializable.deserialize(invokeLambda(modifySpaceEvent));
            assertEquals("OK",response.getStatus());

            final List<Feature> features = new ArrayList<Feature>(){{
                add(FeatureGenerator.generateFeature(new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L), null));
            }};

            ModifyFeaturesEvent modifyFeaturesEvent = new ModifyFeaturesEvent()
                    .withSpace(space)
                    .withConnectorParams(connectorParams)
                    .withTransaction(true)
                    .withInsertFeatures(features);
            invokeLambda(modifyFeaturesEvent);
        }
    }

    protected static void checkIDXTable(int szenario, boolean baselayerSwitch) throws Exception{
        //TODO: Check if we can remove it completely
//        String q = "SELECT * FROM "+ ModifySpace.IDX_STATUS_TABLE_FQN +" WHERE spaceid IN ('"+ BASE1 +"','"+BASE2+"','"+DELTA1+"','"+DELTA2+"');";
//        ObjectNode base1_ref = null;
//        ObjectNode base2_ref = (ObjectNode)DEFAULT_MAPPER.get().readTree("{\"searchableProperties\": {\"search_test_base2\": true}}");
//        ObjectNode delta1_ref = null;
//        ObjectNode delta2_ref = null;
//
//        switch (szenario){
//            //Baseline (base1,base2,delta1,delta2 newly created)
//            case 1:
//                base1_ref = (ObjectNode) DEFAULT_MAPPER.get().readTree("{\"sortableProperties\": [[\"sort_test\"]], \"searchableProperties\": {\"search_test\": true}}");
//                delta1_ref = base1_ref;
//                delta2_ref = base1_ref;
//                break;
//            //Searchable and SortableProperties got updated in Base1
//            case 2:
//                base1_ref = (ObjectNode) DEFAULT_MAPPER.get().readTree("{\"searchableProperties\": {\"search_test\": false,\"search_test2\": true}}");
//                delta1_ref = base1_ref;
//                delta2_ref = base1_ref;
//                break;
//            //Switch Base Layer from delta_2 from base1 to bas2
//            case 3:
//                base1_ref = (ObjectNode) DEFAULT_MAPPER.get().readTree("{\"searchableProperties\": {\"search_test\": false,\"search_test2\": true}}");
//                delta2_ref = base1_ref;
//                delta1_ref = base2_ref;
//        }
//
//        try (final Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection()) {
//            Statement stmt = connection.createStatement();
//            ResultSet resultSet = stmt.executeQuery(q);
//            int i = 0;
//
//            while (resultSet.next()) {
//                i++;
//                String spaceId = resultSet.getString("spaceid");
//                ObjectNode idx_manual = (ObjectNode) DEFAULT_MAPPER.get().readTree(resultSet.getString("idx_manual"));
//                String autoIndexing = resultSet.getString("auto_indexing");
//                boolean idx_creation_finished = resultSet.getBoolean("idx_creation_finished");
//
//                switch (spaceId){
//                    case BASE1:
//                        assertTrue(compareFields(base1_ref, idx_manual));
//                        assertNull(autoIndexing);
//                        break;
//                    case BASE2:
//                        assertTrue(compareFields(base2_ref, idx_manual));
//                        assertNull(autoIndexing);
//                        break;
//                    case DELTA1:
//                        if(!baselayerSwitch) {
//                            /** Inject mocked Auto-Index*/
//                            delta1_ref.put("searchableProperties", ((ObjectNode)delta1_ref.get("searchableProperties")).put("foo", true));
//                        }
//                        assertTrue(compareFields( delta1_ref, idx_manual));
//                        assertEquals("f",autoIndexing);
//                        break;
//                    case DELTA2:
//                        /** Inject mocked Auto-Index*/
//                        delta2_ref.put("searchableProperties", ((ObjectNode) delta2_ref.get("searchableProperties")).put("foo", true));
//
//                        assertTrue(compareFields(delta2_ref, idx_manual));
//                        assertEquals("f",autoIndexing);
//                }
//                assertFalse(idx_creation_finished);
//            }
//            /** Are all entries are present? */
//            assertEquals(4, i);
//        }
    }

    private static boolean compareFields(ObjectNode obj1, ObjectNode obj2) {
        Iterator<String> fieldNames = obj1.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode value1 = obj1.get(fieldName);
            JsonNode value2 = obj2.get(fieldName);

            if (!value1.equals(value2)) {
                return false;
            }
        }
        return true;
    }

    protected static void mockAutoIndexing() throws Exception{
        String q = "CREATE INDEX IF NOT EXISTS idx_base_test_foo_a" +
                " ON public."+ BASE1 +" USING btree" +
                " (((jsondata -> 'properties'::text) -> 'foo'::text));" +
                "COMMENT ON INDEX public.idx_base_test_foo_a" +
                "    IS 'foo';";

        try (final Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute(q);
        }
    }
}
