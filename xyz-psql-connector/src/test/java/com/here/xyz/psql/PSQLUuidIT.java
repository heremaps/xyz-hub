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
    public void testModifyFeatureFailuresWithUUID() throws Exception {
        testModifyFeatureFailures(true);
    }

    @Test
    public void testModifyFeatureFailuresWithoutUUID() throws Exception {
        testModifyFeatureFailures(false);
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
