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

import static io.restassured.path.json.JsonPath.with;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PSQLWriteIT extends PSQLAbstractIT {

    @BeforeClass
    public static void init() throws Exception { initEnv(null); }

    @Before
    public void createTable() throws Exception {
        invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    }

    @After
    public void shutdown() throws Exception { invokeDeleteTestSpace(null); }

    @Test
    public void testTableCreated() throws Exception {
        String response = invokeLambdaFromFile("/events/TestCreateTable.json");
        assertEquals("Check response status", "FeatureCollection", with(response).get("type"));
    }

    @Test
    public void testUpsertFeature() throws Exception {
        invokeLambdaFromFile("/events/InsertFeaturesEvent.json");

        // =========== UPSERT ==========
        String jsonFile = "/events/UpsertFeaturesEvent.json";
        String response = invokeLambdaFromFile(jsonFile);
        LOGGER.info("RAW RESPONSE: " + response);
        String request = IOUtils.toString(this.getClass().getResourceAsStream(jsonFile));
        assertRead(request, response, false);
        LOGGER.info("Upsert feature tested successfully");
    }

    @Test
    public void testCrudFeatureWithHash() throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEventWithHash.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        LOGGER.info("Insert feature tested successfully");

        // =========== UPDATE ==========
        FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
        setPUUID(featureCollection);

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withEnableUUID(true)
                .withUpdateFeatures(featureCollection.getFeatures());

        String updateRequest = mfevent.serialize();
        updateRequest = updateRequest.replaceAll("Tesla", "Honda");
        String updateResponse = invokeLambda(updateRequest);

        FeatureCollection responseCollection = XyzSerializable.deserialize(updateResponse);
        setPUUID(responseCollection);

        assertUpdate(updateRequest, updateResponse, true);
        assertUpdate(updateRequest, updateResponse, true);
        LOGGER.info("Update feature tested successfully");
    }

    @Test
    public void testCrudFeatureWithTransaction() throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        LOGGER.info("Insert feature tested successfully");

        // =========== UPDATE ==========
        FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
        setPUUID(featureCollection);

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withEnableUUID(true)
                .withUpdateFeatures(featureCollection.getFeatures());

        String updateResponse = invokeLambda(mfevent.serialize());

        assertUpdate(mfevent.serialize(), updateResponse, true);
        LOGGER.info("Update feature tested successfully");

        // =========== LoadFeaturesEvent ==========
        String loadFeaturesEvent = "/events/LoadFeaturesEvent.json";
        String loadResponse = invokeLambdaFromFile(loadFeaturesEvent);
        featureCollection = XyzSerializable.deserialize(loadResponse);
        assertNotNull(featureCollection.getFeatures());
        assertEquals(1, featureCollection.getFeatures().size());
        assertEquals("test", featureCollection.getFeatures().get(0).getId());

        // =========== DELETE ==========
        final String deleteId = featureCollection.getFeatures().get(0).getId();

        mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withEnableUUID(true)
                .withDeleteFeatures(new HashMap<String,String>(){{ put(deleteId, null);}});

        String deleteResponse = invokeLambda(mfevent.serialize());
        featureCollection = XyzSerializable.deserialize(deleteResponse);
        assertNotNull(featureCollection.getFeatures());
        assertEquals(0, featureCollection.getFeatures().size());
        assertEquals(1, featureCollection.getDeleted().size());
    }

    @Test
    public void testCrudFeatureWithoutHash() throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEvent.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        LOGGER.info("RAW RESPONSE: " + insertResponse);
        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, false);
        LOGGER.info("Insert feature tested successfully");

        // =========== COUNT ==========
        String statsResponse = invokeLambdaFromFile("/events/GetStatisticsEvent.json");
        assertCount(insertRequest, statsResponse);
        LOGGER.info("Count feature tested successfully");

        // =========== SEARCH ==========
        String searchResponse = invokeLambdaFromFile("/events/SearchForFeaturesEvent.json");
        assertRead(insertRequest, searchResponse, false);
        LOGGER.info("Search feature tested successfully");

        // =========== SEARCH WITH PROPERTIES ========
        String searchPropertiesResponse = invokeLambdaFromFile("/events/SearchForFeaturesByPropertiesEvent.json");
        assertRead(insertRequest, searchPropertiesResponse, false);
        LOGGER.info("Search Properties feature tested successfully");

        // =========== UPDATE ==========
        FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
        String featuresList = XyzSerializable.serialize(featureCollection.getFeatures(), new TypeReference<List<Feature>>() {});

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withUpdateFeatures(featureCollection.getFeatures());

        String updateRequest = mfevent.serialize();
        updateRequest = updateRequest.replaceAll("Tesla", "Honda");
        String updateResponse = invokeLambda(updateRequest);

        assertUpdate(updateRequest, updateResponse, false);
        LOGGER.info("Update feature tested successfully");
    }

    @Test
    public void testNullGeometry() throws Exception {

        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertNullGeometry.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        LOGGER.info("RAW RESPONSE: " + insertResponse);
        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, false);
        LOGGER.info("Preparation: Insert features");

        // =========== Validate that "geometry":null is serialized ==========
        String response = invokeLambdaFromFile("/events/GetFeaturesByIdEvent.json");
        assertTrue(response.indexOf("\"geometry\":null") > 0);
    }

    @Test
    public void testModifyFeaturesDefault() throws Exception {
        testModifyFeatures(false);
    }

    @Test
    public void testModifyFeaturesWithOldStates() throws Exception {
        testModifyFeatures(true);
    }

    protected void assertUpdate(String updateRequest, String response, boolean checkGuid) throws Exception {
        ModifyFeaturesEvent gsModifyFeaturesEvent = XyzSerializable.deserialize(updateRequest);
        FeatureCollection featureCollection = XyzSerializable.deserialize(response);
        for (int i = 0; i < gsModifyFeaturesEvent.getUpdateFeatures().size(); i++) {
            Feature expectedFeature = gsModifyFeaturesEvent.getUpdateFeatures().get(i);
            Feature actualFeature = featureCollection.getFeatures().get(i);
            assertTrue("Check geometry", jsonCompare(expectedFeature.getGeometry(), actualFeature.getGeometry()));
            assertEquals("Check name", (String) expectedFeature.getProperties().get("name"), actualFeature.getProperties().get("name"));
            assertNotNull("Check id", actualFeature.getId());

            assertTrue("Check tags", jsonCompare(expectedFeature.getProperties().getXyzNamespace().getTags(),
                    actualFeature.getProperties().getXyzNamespace().getTags()));
            assertEquals("Check space", gsModifyFeaturesEvent.getSpace(), actualFeature.getProperties().getXyzNamespace().getSpace());
            assertNotEquals("Check createdAt", 0L, actualFeature.getProperties().getXyzNamespace().getCreatedAt());
            assertNotEquals("Check updatedAt", 0L, actualFeature.getProperties().getXyzNamespace().getUpdatedAt());
            if (checkGuid) {
                assertNotNull("Check uuid", actualFeature.getProperties().getXyzNamespace().getUuid()); // After version 0.2.0
                assertNotNull("Check uuid", actualFeature.getProperties().getXyzNamespace().getUuid());
            } else {
                assertNull("Check parent", actualFeature.getProperties().getXyzNamespace().getPuuid());
                assertNull("Check uuid", actualFeature.getProperties().getXyzNamespace().getUuid());
            }
        }
    }

    protected void assertCount(String insertRequest, String countResponse) {
        if (!with(countResponse).getBoolean("count.estimated")) {
            assertEquals("Check inserted feature count vs fetched count",
                with(insertRequest).getString("$.insertFeatures.length()"),
                with(countResponse).getString("$.count.value"));
        }
    }

    @SuppressWarnings({"rawtypes"})
    protected void testModifyFeatures(boolean includeOldStates) throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEvent.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        LOGGER.info("RAW RESPONSE: " + insertResponse);
        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, false);
        List<Map> originalFeatures = with(insertResponse).get("features");

        List<String> ids = with(insertResponse).get("features.id");
        LOGGER.info("Preparation: Inserted features {}", ids);
        FeatureCollection inserted = XyzSerializable.deserialize(insertResponse);

        // =========== UPDATE ==========
        LOGGER.info("Modify features");
        ModifyFeaturesEvent updateEvent = new ModifyFeaturesEvent()
            .withSpace("foo")
            .withParams(Collections.singletonMap("includeOldStates", includeOldStates));
        inserted.getFeatures().forEach((Feature feature) -> {
            feature.getProperties().put("test", "updated");
        });
        updateEvent.setUpdateFeatures(inserted.getFeatures());
        String updateFeaturesResponse = invokeLambda(updateEvent.toString());
        assertNoErrorInResponse(updateFeaturesResponse);

        List features = with(updateFeaturesResponse).get("features");
        assertNotNull("'features' element in ModifyFeaturesResponse is missing", features);
        assertTrue("'features' element in ModifyFeaturesResponse is empty", features.size() > 0);

        List oldFeatures = with(updateFeaturesResponse).get("oldFeatures");
        if (includeOldStates) {
            assertNotNull("'oldFeatures' element in ModifyFeaturesResponse is missing", oldFeatures);
            assertTrue("'oldFeatures' element in ModifyFeaturesResponse is empty", oldFeatures.size() > 0);
            assertEquals(oldFeatures, originalFeatures);
        } else if (oldFeatures != null) {
            assertEquals("unexpected oldFeatures in ModifyFeaturesResponse", 0, oldFeatures.size());
        }

        // =========== DELETE ==========
        ModifyFeaturesEvent deleteEvent = new ModifyFeaturesEvent()
            .withSpace("foo")
            .withParams(Collections.singletonMap("includeOldStates", includeOldStates));

        Map<String, String> idsMap = new HashMap<>();
        ids.forEach(id -> idsMap.put(id, null));
        deleteEvent.setDeleteFeatures(idsMap);

        String deleteResponse = invokeLambda(deleteEvent.toString());
        assertNoErrorInResponse(deleteResponse);
        oldFeatures = with(deleteResponse).get("oldFeatures");
        if (includeOldStates) {
            assertNotNull("'oldFeatures' element in ModifyFeaturesResponse is missing", oldFeatures);
            assertTrue("'oldFeatures' element in ModifyFeaturesResponse is empty", oldFeatures.size() > 0);
            assertEquals(oldFeatures, features);
        } else if (oldFeatures != null) {
            assertEquals("unexpected oldFeatures in ModifyFeaturesResponse", 0, oldFeatures.size());
        }

        LOGGER.info("Modify features tested successfully");
    }
}
