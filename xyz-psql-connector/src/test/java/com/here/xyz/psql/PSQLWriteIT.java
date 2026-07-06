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

import static io.restassured.path.json.JsonPath.with;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PSQLWriteIT extends PSQLAbstractIT {

    @BeforeEach
    public void createTable() throws Exception {
        invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    }

    @AfterEach
    public void shutdown() throws Exception { invokeDeleteTestSpace(null); }

    @Test
    public void testTableCreated() throws Exception {
        String response = invokeLambdaFromFile("/events/TestCreateTable.json");
        assertEquals( "FeatureCollection", with(response).get("type"),"Check response status");
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
        FeatureCollection featureCollection = deserializeResponse(insertResponse);

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withConflictDetectionEnabled(true)
                .withUpdateFeatures(featureCollection.getFeatures());

        String updateRequest = mfevent.serialize();
        updateRequest = updateRequest.replaceAll("Tesla", "Honda");
        String updateResponse = invokeLambda(XyzSerializable.deserialize(updateRequest, Event.class));

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
        FeatureCollection featureCollection = deserializeResponse(insertResponse);

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withConflictDetectionEnabled(true)
                .withUpdateFeatures(featureCollection.getFeatures());

        String updateResponse = invokeLambda(mfevent);

        assertUpdate(mfevent.serialize(), updateResponse, true);
        LOGGER.info("Update feature tested successfully");

        // =========== LoadFeaturesEvent ==========
        String loadFeaturesEvent = "/events/LoadFeaturesEvent.json";
        String loadResponse = invokeLambdaFromFile(loadFeaturesEvent);
        featureCollection = deserializeResponse(loadResponse);
        assertNotNull(featureCollection.getFeatures());
        assertEquals(1, featureCollection.getFeatures().size());
        assertEquals("test", featureCollection.getFeatures().get(0).getId());

        // =========== DELETE ==========
        final String deleteId = featureCollection.getFeatures().get(0).getId();

        mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withConflictDetectionEnabled(true)
                .withDeleteFeatures(new HashMap<String,String>(){{ put(deleteId, null);}});

        String deleteResponse = invokeLambda(mfevent);
        featureCollection = deserializeResponse(deleteResponse);
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
        XyzSerializable.serialize(featureCollection.getFeatures(), new TypeReference<List<Feature>>() {});

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withUpdateFeatures(featureCollection.getFeatures());

        String updateRequest = mfevent.serialize();
        updateRequest = updateRequest.replaceAll("Tesla", "Honda");
        String updateResponse = invokeLambda(XyzSerializable.deserialize(updateRequest, Event.class));

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
        // s. https://datatracker.ietf.org/doc/html/rfc7946#section-3.2
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

    protected void assertUpdate(String updateRequest, String response, boolean checkVersion) throws Exception {
        ModifyFeaturesEvent gsModifyFeaturesEvent = XyzSerializable.deserialize(updateRequest);
        FeatureCollection featureCollection = deserializeResponse(response);
        for (int i = 0; i < gsModifyFeaturesEvent.getUpdateFeatures().size(); i++) {
            Feature expectedFeature = gsModifyFeaturesEvent.getUpdateFeatures().get(i);
            Feature actualFeature = featureCollection.getFeatures().get(i);
            assertTrue( jsonCompare(expectedFeature.getGeometry(), actualFeature.getGeometry()),"Check geometry");
            assertEquals((String) expectedFeature.getProperties().get("name"), actualFeature.getProperties().get("name"),"Check name");
            assertNotNull( actualFeature.getId(),"Check id");

            assertTrue( jsonCompare(expectedFeature.getProperties().getXyzNamespace().getTags(),
                    actualFeature.getProperties().getXyzNamespace().getTags()), "Check tags");
            assertEquals( gsModifyFeaturesEvent.getSpace(), actualFeature.getProperties().getXyzNamespace().getSpace(), "Check space");
            assertNotEquals( 0L, actualFeature.getProperties().getXyzNamespace().getCreatedAt(),"Check createdAt");
            assertNotEquals( 0L, actualFeature.getProperties().getXyzNamespace().getUpdatedAt(), "Check updatedAt");
            assertNotEquals( -1, actualFeature.getProperties().getXyzNamespace().getVersion(), "Check version");
        }
    }

    protected void assertCount(String insertRequest, String countResponse) {
        if (!with(countResponse).getBoolean("count.estimated")) {
            assertEquals(
                with(insertRequest).getString("$.insertFeatures.length()"),
                with(countResponse).getString("$.count.value"), "Check inserted feature count vs fetched count");
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
        String updateFeaturesResponse = invokeLambda(updateEvent);
        assertNoErrorInResponse(updateFeaturesResponse);

        List features = with(updateFeaturesResponse).get("features");
        assertNotNull( features, "'features' element in ModifyFeaturesResponse is missing");
        assertTrue( features.size() > 0,"'features' element in ModifyFeaturesResponse is empty");

        List existingFeatures = with(updateFeaturesResponse).get("oldFeatures");
        if (includeOldStates) {
            assertNotNull( existingFeatures,"'oldFeatures' element in ModifyFeaturesResponse is missing");
            assertTrue( existingFeatures.size() > 0,"'oldFeatures' element in ModifyFeaturesResponse is empty");
            assertEquals(existingFeatures, originalFeatures);
        } else if (existingFeatures != null) {
            assertEquals( 0, existingFeatures.size(), "unexpected oldFeatures in ModifyFeaturesResponse");
        }

        // =========== DELETE ==========
        ModifyFeaturesEvent deleteEvent = new ModifyFeaturesEvent()
            .withSpace("foo")
            .withParams(Collections.singletonMap("includeOldStates", includeOldStates));

        Map<String, String> idsMap = new HashMap<>();
        ids.forEach(id -> idsMap.put(id, null));
        deleteEvent.setDeleteFeatures(idsMap);

        String deleteResponse = invokeLambda(deleteEvent);
        assertNoErrorInResponse(deleteResponse);
        existingFeatures = with(deleteResponse).get("oldFeatures");
        if (includeOldStates) {
            assertNotNull( existingFeatures, "'oldFeatures' element in ModifyFeaturesResponse is missing");
            assertTrue( existingFeatures.size() > 0, "'oldFeatures' element in ModifyFeaturesResponse is empty");
//            existingFeatures.forEach(f -> f.);
            assertEquals(existingFeatures, features);
        } else if (existingFeatures != null) {
            assertEquals( 0, existingFeatures.size(), "unexpected oldFeatures in ModifyFeaturesResponse");
        }

        LOGGER.info("Modify features tested successfully");
    }
}
