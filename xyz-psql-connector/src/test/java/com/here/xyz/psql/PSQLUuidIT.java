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

import com.amazonaws.util.IOUtils;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.events.SearchForFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class PSQLUuidIT extends PSQLAbstractIT {

    @BeforeClass
    public static void init() throws Exception { initEnv(null); }

    @Before
    public void createTable() throws Exception {
        invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    }

    @After
    public void shutdown() throws Exception { invokeDeleteTestSpace(null); }

    @Test
    public void testStreamUUIDCases() throws Exception {
        XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L).withUuid("4e16d729-e4f7-4ea9-b0da-4af4ac53c5c4");

        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        LOGGER.info("Insert feature tested successfully");

        // =========== UPDATE With wrong UUID ==========
        FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
        for (Feature feature : featureCollection.getFeatures()) {
            feature.getProperties().put("foo", "bar");
        }

        String modifiedFeatureId = featureCollection.getFeatures().get(1).getId();
        featureCollection.getFeatures().get(1).getProperties().getXyzNamespace().setUuid("wrong");

        Feature newFeature = new Feature().withId("test2").withProperties(new Properties().withXyzNamespace(xyzNamespace));
        List<Feature> insertFeatureList = new ArrayList<>();
        insertFeatureList.add(newFeature);

        List<String> idList = featureCollection.getFeatures().stream().map(Feature::getId).collect(Collectors.toList());
        idList.add("test2");

        setPUUID(featureCollection);

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(false)
                .withEnableUUID(true)
                .withUpdateFeatures(featureCollection.getFeatures())
                .withInsertFeatures(insertFeatureList);

        String response = invokeLambda(mfevent.serialize());
        FeatureCollection responseCollection = XyzSerializable.deserialize(response);

        assertEquals(3, responseCollection.getFeatures().size());
        assertEquals(1, responseCollection.getFailed().size());
        assertTrue(responseCollection.getFailed().get(0).getId().equalsIgnoreCase(modifiedFeatureId));

        List<String> inserted = responseCollection.getInserted();

        FeatureCollection.ModificationFailure failure = responseCollection.getFailed().get(0);
        assertEquals(DatabaseWriter.UPDATE_ERROR_UUID,failure.getMessage());
        assertEquals(modifiedFeatureId,failure.getId());

        //Check if updates got written (correct UUID)
        SearchForFeaturesEvent searchEvent = (SearchForFeaturesEvent) new SearchForFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo");

        String eventJson = searchEvent.serialize();
        String searchResponse = invokeLambda(eventJson);
        responseCollection = XyzSerializable.deserialize(searchResponse);

        for (Feature feature : responseCollection.getFeatures()) {
            // The new Feature and the failed updated one should not have the property foo
            if(feature.getId().equalsIgnoreCase(modifiedFeatureId) || feature.getId().equalsIgnoreCase(inserted.get(0)))
                assertNull(feature.getProperties().get("foo"));
            else
                assertEquals("bar",feature.getProperties().get("foo"));
            assertTrue(idList.contains(failure.getId()));
        }

        // =========== UPDATE With correct UUID ==========
        for (Feature feature : responseCollection.getFeatures()) {
            feature.getProperties().put("foo","bar2");
        }

        setPUUID(responseCollection);

        mfevent.setUpdateFeatures(responseCollection.getFeatures());
        mfevent.setInsertFeatures(new ArrayList<>());

        response = invokeLambda(mfevent.serialize());
        responseCollection = XyzSerializable.deserialize(response);
        assertEquals(4, responseCollection.getFeatures().size());
        assertEquals(4, responseCollection.getUpdated().size());
        assertNull(responseCollection.getFailed());

        for (Feature feature : responseCollection.getFeatures()) {
            assertEquals("bar2",feature.getProperties().get("foo"));
        }

        // =========== Delete With wrong UUID ==========
        Map<String,String> idUUIDMap = new HashMap<>();
        Map<String,String> idMap = new HashMap<>();
        //get current UUIDS
        for (Feature feature : responseCollection.getFeatures()) {
            idUUIDMap.put(feature.getId(),feature.getProperties().getXyzNamespace().getUuid());
            idMap.put(feature.getId(),null);
        }

        idUUIDMap.put(modifiedFeatureId, "wrong");
        mfevent.setUpdateFeatures(new ArrayList<>());
        mfevent.setDeleteFeatures(idUUIDMap);

        response = invokeLambda(mfevent.serialize());
        responseCollection = XyzSerializable.deserialize(response);

        assertEquals(0, responseCollection.getFeatures().size());
        assertEquals(3, responseCollection.getDeleted().size());
        assertEquals(1, responseCollection.getFailed().size());

        // Only the feature with wrong UUID should have failed
        failure = responseCollection.getFailed().get(0);
        assertEquals(DatabaseWriter.DELETE_ERROR_UUID,failure.getMessage());
        assertEquals(modifiedFeatureId, failure.getId());

        // Check if deletes are got performed
        searchResponse = invokeLambda(eventJson);
        responseCollection = XyzSerializable.deserialize(searchResponse);
        assertEquals(1, responseCollection.getFeatures().size());

        // =========== Delete without UUID ==========
        mfevent.setDeleteFeatures(idMap);
        response = invokeLambda(mfevent.serialize());
        responseCollection = XyzSerializable.deserialize(response);

        assertEquals(0, responseCollection.getFeatures().size());
        assertEquals(1, responseCollection.getDeleted().size());
        assertEquals(modifiedFeatureId, (responseCollection.getDeleted().get(0)));
        // Check if deletes are got performed
        searchResponse = invokeLambda(eventJson);
        responseCollection = XyzSerializable.deserialize(searchResponse);
        assertEquals(0, responseCollection.getFeatures().size());
    }

    @Test
    public void testModifyFeatureFailuresWithUUID() throws Exception {
        testModifyFeatureFailures(true);
    }

    @Test
    public void testModifyFeatureFailuresWithoutUUID() throws Exception {
        testModifyFeatureFailures(false);
    }

    @Test
    public void testTransactionalUUIDCases() throws Exception {
        XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);

        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEventTransactional.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, true);
        LOGGER.info("Insert feature tested successfully");

        // =========== UPDATE With wrong UUID ==========
        FeatureCollection featureCollection = XyzSerializable.deserialize(insertResponse);
        for (Feature feature : featureCollection.getFeatures()) {
            feature.getProperties().put("foo","bar");
        }

        String modifiedFeatureId = featureCollection.getFeatures().get(1).getId();
        featureCollection.getFeatures().get(1).getProperties().getXyzNamespace().setUuid("wrong");

        Feature newFeature = new Feature().withId("test2").withProperties(new Properties().withXyzNamespace(xyzNamespace));
        List<Feature> insertFeatureList = new ArrayList<>();
        insertFeatureList.add(newFeature);

        List<String> idList = featureCollection.getFeatures().stream().map(Feature::getId).collect(Collectors.toList());
        idList.add("test2");

        setPUUID(featureCollection);

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(true)
                .withEnableUUID(true)
                .withUpdateFeatures(featureCollection.getFeatures())
                .withInsertFeatures(insertFeatureList);

        String response = invokeLambda(mfevent.serialize());

        // Transaction should have failed
        ErrorResponse errorResponse = XyzSerializable.deserialize(response);
        assertEquals(XyzError.CONFLICT, errorResponse.getError());
        ArrayList failedList = ((ArrayList)errorResponse.getErrorDetails().get("FailedList"));
        assertEquals(1, failedList.size());

        HashMap<String,String> failure1 = ((HashMap<String,String>)failedList.get(0));
        assertEquals(modifiedFeatureId, failure1.get("id"));
        assertEquals(DatabaseWriter.UPDATE_ERROR_UUID, failure1.get("message"));

        //Check if nothing got written
        SearchForFeaturesEvent searchEvent = (SearchForFeaturesEvent) new SearchForFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo");

        String eventJson = searchEvent.serialize();
        String searchResponse = invokeLambda(eventJson);
        FeatureCollection responseCollection = XyzSerializable.deserialize(searchResponse);

        for (Feature feature : responseCollection.getFeatures()) {
            assertNull(feature.getProperties().get("foo"));
        }

        // =========== UPDATE With correct UUID ==========
        for (Feature feature : responseCollection.getFeatures()) {
            feature.getProperties().put("foo","bar");
        }

        setPUUID(responseCollection);

        mfevent.setUpdateFeatures(responseCollection.getFeatures());
        mfevent.setInsertFeatures(new ArrayList<>());

        response = invokeLambda(mfevent.serialize());
        responseCollection = XyzSerializable.deserialize(response);
        assertEquals(3, responseCollection.getFeatures().size());
        assertEquals(3, responseCollection.getUpdated().size());
        assertNull(responseCollection.getFailed());

        // Check returned FeatureCollection
        for (Feature feature : responseCollection.getFeatures()) {
            assertEquals("bar",feature.getProperties().get("foo"));
        }

        // Check if updates got performed
        searchResponse = invokeLambda(eventJson);
        responseCollection = XyzSerializable.deserialize(searchResponse);
        assertEquals(3, responseCollection.getFeatures().size());

        for (Feature feature : responseCollection.getFeatures()) {
            assertEquals("bar",feature.getProperties().get("foo"));
        }

        // =========== Delete With wrong UUID ==========
        Map<String,String> idUUIDMap = new HashMap<>();
        Map<String,String> idMap = new HashMap<>();
        //get current UUIDS
        for (Feature feature : responseCollection.getFeatures()) {
            idUUIDMap.put(feature.getId(),feature.getProperties().getXyzNamespace().getUuid());
            idMap.put(feature.getId(),null);
        }

        idUUIDMap.put(modifiedFeatureId, "wrong");
        mfevent.setUpdateFeatures(new ArrayList<>());
        mfevent.setDeleteFeatures(idUUIDMap);

        response = invokeLambda(mfevent.serialize());
        // Transaction should have failed
        errorResponse = XyzSerializable.deserialize(response);
        assertEquals(XyzError.CONFLICT, errorResponse.getError());
        failedList = ((ArrayList)errorResponse.getErrorDetails().get("FailedList"));
        assertEquals(1,failedList.size());

        failure1 = ((HashMap<String,String>)failedList.get(0));
        assertEquals(modifiedFeatureId, failure1.get("id"));
        assertEquals(DatabaseWriter.DELETE_ERROR_UUID, failure1.get("message"));

        // Check if deletes has failed
        searchResponse = invokeLambda(eventJson);
        responseCollection = XyzSerializable.deserialize(searchResponse);
        assertEquals(3, responseCollection.getFeatures().size());

        // =========== Delete without UUID ==========
        mfevent.setDeleteFeatures(idMap);
        response = invokeLambda(mfevent.serialize());
        responseCollection = XyzSerializable.deserialize(response);

        assertEquals(0, responseCollection.getFeatures().size());
        assertNull(responseCollection.getFailed());

        // Check if deletes are got performed
        searchResponse = invokeLambda(eventJson);
        responseCollection = XyzSerializable.deserialize(searchResponse);
        assertEquals(0, responseCollection.getFeatures().size());
    }

    protected void testModifyFeatureFailures(boolean withUUID) throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = withUUID ? "/events/InsertFeaturesEventTransactional.json" : "/events/InsertFeaturesEvent.json";
        final String insertResponse = invokeLambdaFromFile(insertJsonFile);
        final String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        final FeatureCollection insertRequestCollection = XyzSerializable.deserialize(insertResponse);
        assertRead(insertRequest, insertResponse, withUUID);
        LOGGER.info("Insert feature tested successfully");

        // =========== DELETE NOT EXISTING FEATURE ==========
        //Stream
        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(false)
                .withDeleteFeatures(Collections.singletonMap("doesnotexist", null));
        if(withUUID)
            mfevent.setEnableUUID(true);

        String response = invokeLambda(mfevent.serialize());
        FeatureCollection responseCollection = XyzSerializable.deserialize(response);
        assertEquals("doesnotexist", responseCollection.getFailed().get(0).getId());
        assertEquals(0,responseCollection.getFeatures().size());
        assertNull(responseCollection.getUpdated());
        assertNull(responseCollection.getInserted());
        assertNull(responseCollection.getDeleted());

        if(withUUID)
            assertEquals(DatabaseWriter.DELETE_ERROR_UUID, responseCollection.getFailed().get(0).getMessage());
        else
            assertEquals(DatabaseWriter.DELETE_ERROR_NOT_EXISTS, responseCollection.getFailed().get(0).getMessage());

        //Transactional
        mfevent.setTransaction(true);
        response = invokeLambda(mfevent.serialize());

        // Transaction should have failed
        ErrorResponse errorResponse = XyzSerializable.deserialize(response);
        assertEquals(XyzError.CONFLICT, errorResponse.getError());
        ArrayList failedList = ((ArrayList)errorResponse.getErrorDetails().get("FailedList"));
        assertEquals(1, failedList.size());

        HashMap<String,String> failure1 = ((HashMap<String,String>)failedList.get(0));
        assertEquals("doesnotexist", failure1.get("id"));

        if(withUUID)
            assertEquals(DatabaseWriter.DELETE_ERROR_UUID, failure1.get("message"));
        else
            assertEquals(DatabaseWriter.DELETE_ERROR_NOT_EXISTS, failure1.get("message"));

        Feature existing = insertRequestCollection.getFeatures().get(0);
        existing.getProperties().getXyzNamespace().setPuuid(existing.getProperties().getXyzNamespace().getUuid());
        mfevent.setDeleteFeatures(new HashMap<>());
//        // =========== INSERT EXISTING FEATURE ==========
//        //Stream
//        mfevent.setInsertFeatures(new ArrayList<Feature>(){{add(existing);}});
//        mfevent.setTransaction(false);
//        response = invokeLambda(mfevent.serialize());
//        responseCollection = XyzSerializable.deserialize(response);
//        assertEquals(existing.getId(), responseCollection.getFailed().get(0).getId());
//        assertEquals(DatabaseWriter.INSERT_ERROR_GENERAL, responseCollection.getFailed().get(0).getMessage());
//        assertEquals(0,responseCollection.getFeatures().size());
//        assertNull(responseCollection.getUpdated());
//        assertNull(responseCollection.getInserted());
//        assertNull(responseCollection.getDeleted());
//
//        //Transactional
//        mfevent.setTransaction(true);
//        response = invokeLambda(mfevent.serialize());
//
//        errorResponse = XyzSerializable.deserialize(response);
//        assertEquals(XyzError.CONFLICT, errorResponse.getError());
//        failedList = ((ArrayList)errorResponse.getErrorDetails().get("FailedList"));
//        assertEquals(0, failedList.size());
//        assertEquals(DatabaseWriter.TRANSACTION_ERROR_GENERAL, errorResponse.getErrorMessage());

        // =========== UPDATE NOT EXISTING FEATURE ==========
        //Stream
        //Change ID to not existing one
        existing.setId("doesnotexist");
        mfevent.setInsertFeatures(new ArrayList<>());
        mfevent.setUpdateFeatures(new ArrayList<Feature>(){{add(existing);}});
        mfevent.setTransaction(false);

        response = invokeLambda(mfevent.serialize());
        responseCollection = XyzSerializable.deserialize(response);
        assertEquals(existing.getId(), responseCollection.getFailed().get(0).getId());
        assertEquals(0,responseCollection.getFeatures().size());
        assertNull(responseCollection.getUpdated());
        assertNull(responseCollection.getInserted());
        assertNull(responseCollection.getDeleted());

        if(withUUID)
            assertEquals(DatabaseWriter.UPDATE_ERROR_UUID, responseCollection.getFailed().get(0).getMessage());
        else
            assertEquals(DatabaseWriter.UPDATE_ERROR_NOT_EXISTS, responseCollection.getFailed().get(0).getMessage());

        //Transactional
        mfevent.setTransaction(true);
        response = invokeLambda(mfevent.serialize());

        errorResponse = XyzSerializable.deserialize(response);
        assertEquals(XyzError.CONFLICT, errorResponse.getError());
        failedList = ((ArrayList)errorResponse.getErrorDetails().get("FailedList"));
        assertEquals(1, failedList.size());

        failure1 = ((HashMap<String,String>)failedList.get(0));
        assertEquals("doesnotexist", failure1.get("id"));

        if(withUUID)
            assertEquals(DatabaseWriter.UPDATE_ERROR_UUID, failure1.get("message"));
        else
            assertEquals(DatabaseWriter.UPDATE_ERROR_NOT_EXISTS, failure1.get("message"));
    }
}
