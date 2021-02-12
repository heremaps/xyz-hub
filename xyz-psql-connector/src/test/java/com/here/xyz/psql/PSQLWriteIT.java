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

import com.amazonaws.util.IOUtils;
import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.jayway.jsonpath.JsonPath;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;

public class PSQLWriteIT extends PSQLAbstractIT {

    @BeforeClass
    public static void init() throws Exception { initEnv(null); }

    @Before
    public void removeTestSpaces() throws Exception { deleteTestSpace(null); }

    @After
    public void shutdown() throws Exception { shutdownEnv(null); }

    @Test
    public void testTableCreated() throws Exception {
        String response = invokeLambdaFromFile("/events/TestCreateTable.json");
        assertEquals("Check response status", JsonPath.read(response, "$.type").toString(), "FeatureCollection");
    }

    @Test
    public void testUpsertFeature() throws Exception {
        invokeLambdaFromFile("/events/InsertFeaturesEvent.json");

        // =========== UPSERT ==========
        String jsonFile = "/events/UpsertFeaturesEvent.json";
        String response = invokeLambdaFromFile(jsonFile);
        logger.info("RAW RESPONSE: " + response);
        String request = IOUtils.toString(GSContext.class.getResourceAsStream(jsonFile));
        assertRead(request, response, false);
        logger.info("Upsert feature tested successfully");
    }

    @Test
    public void testCrudFeatureWithHash() throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEventWithHash.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        logger.info("Insert feature tested successfully");

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
        logger.info("Update feature tested successfully");
    }

    @Test
    public void testCrudFeatureWithTransaction() throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        logger.info("Insert feature tested successfully");

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
        logger.info("Update feature tested successfully");

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
        logger.info("RAW RESPONSE: " + insertResponse);
        String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, false);
        logger.info("Insert feature tested successfully");

        // =========== COUNT ==========
        String countResponse = invokeLambdaFromFile("/events/CountFeaturesEvent.json");
        assertCount(insertRequest, countResponse);
        logger.info("Count feature tested successfully");

        // =========== SEARCH ==========
        String searchResponse = invokeLambdaFromFile("/events/SearchForFeaturesEvent.json");
        assertRead(insertRequest, searchResponse, false);
        logger.info("Search feature tested successfully");

        // =========== SEARCH WITH PROPERTIES ========
        String searchPropertiesResponse = invokeLambdaFromFile("/events/SearchForFeaturesByPropertiesEvent.json");
        assertRead(insertRequest, searchPropertiesResponse, false);
        logger.info("Search Properties feature tested successfully");

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
        logger.info("Update feature tested successfully");

        // =========== DELETE FEATURES ==========
        invokeLambdaFromFile("/events/DeleteFeaturesByTagEvent.json");
        logger.info("Delete feature tested successfully");
    }

    @Test
    public void testNullGeometry() throws Exception {

        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertNullGeometry.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        logger.info("RAW RESPONSE: " + insertResponse);
        String insertRequest = IOUtils.toString(GSContext.class.getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, false);
        logger.info("Preparation: Insert features");

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
}
