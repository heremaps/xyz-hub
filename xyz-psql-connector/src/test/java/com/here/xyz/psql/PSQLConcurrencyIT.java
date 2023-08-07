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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import com.amazonaws.util.IOUtils;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.*;

public class PSQLConcurrencyIT extends PSQLAbstractIT {

    @BeforeClass
    public static void init() throws Exception { initEnv(null); }

    @Before
    public void createTable() throws Exception {
        invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    }

    @After
    public void shutdown() throws Exception { invokeDeleteTestSpace(null); }

    @Test
    public void testModifyFeatureFailuresWithConflictDetection() throws Exception {
        testModifyFeatureFailures(true);
    }

    @Test
    public void testModifyFeatureFailuresWithoutConflictDetection() throws Exception {
        testModifyFeatureFailures(false);
    }

    protected void testModifyFeatureFailures(boolean withConflictDetection) throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = withConflictDetection ? "/events/InsertFeaturesEventTransactional.json" : "/events/InsertFeaturesEvent.json";
        final String insertResponse = invokeLambdaFromFile(insertJsonFile);
        final String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        final FeatureCollection insertRequestCollection = XyzSerializable.deserialize(insertResponse);
        assertRead(insertRequest, insertResponse, withConflictDetection);
        LOGGER.info("Insert feature tested successfully");

        // =========== DELETE NOT EXISTING FEATURE ==========
        //Stream
        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
                .withConnectorParams(defaultTestConnectorParams)
                .withSpace("foo")
                .withTransaction(false)
                .withDeleteFeatures(Collections.singletonMap("doesnotexist", null));
        if (withConflictDetection)
            mfevent.setConflictDetectionEnabled(true);

        String response = invokeLambda(mfevent.serialize());
        FeatureCollection responseCollection = XyzSerializable.deserialize(response);
        assertEquals("doesnotexist", responseCollection.getFailed().get(0).getId());
        assertEquals(0,responseCollection.getFeatures().size());
        assertNull(responseCollection.getUpdated());
        assertNull(responseCollection.getInserted());
        assertNull(responseCollection.getDeleted());

        if (withConflictDetection)
            assertEquals(DatabaseWriter.DELETE_ERROR_CONCURRENCY, responseCollection.getFailed().get(0).getMessage());
        else
            assertEquals(DatabaseWriter.DELETE_ERROR_NOT_EXISTS, responseCollection.getFailed().get(0).getMessage());

        //Transactional
        mfevent.setTransaction(true);
        response = invokeLambda(mfevent.serialize());

        // Transaction should have failed
        ErrorResponse errorResponse = XyzSerializable.deserialize(response);
        assertEquals(XyzError.CONFLICT, errorResponse.getError());
        List<Map<String, String>> failedList = (List<Map<String, String>>) errorResponse.getErrorDetails().get("FailedList");
        assertEquals(1, failedList.size());

        Map<String, String> failure1 = failedList.get(0);
        assertEquals("doesnotexist", failure1.get("id"));

        if (withConflictDetection)
            assertEquals(DatabaseWriter.DELETE_ERROR_CONCURRENCY, failure1.get("message"));
        else
            assertEquals(DatabaseWriter.DELETE_ERROR_NOT_EXISTS, failure1.get("message"));

        Feature existing = insertRequestCollection.getFeatures().get(0);
        existing.getProperties().getXyzNamespace().setVersion(existing.getProperties().getXyzNamespace().getVersion());;
        mfevent.setDeleteFeatures(Collections.emptyMap());
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
        mfevent.setInsertFeatures(Collections.emptyList());
        mfevent.setUpdateFeatures(Collections.singletonList(existing));
        mfevent.setTransaction(false);

        response = invokeLambda(mfevent.serialize());
        responseCollection = XyzSerializable.deserialize(response);
        assertEquals(existing.getId(), responseCollection.getFailed().get(0).getId());
        assertEquals(0,responseCollection.getFeatures().size());
        assertNull(responseCollection.getUpdated());
        assertNull(responseCollection.getInserted());
        assertNull(responseCollection.getDeleted());

        if (withConflictDetection)
            assertEquals(DatabaseWriter.UPDATE_ERROR_CONCURRENCY, responseCollection.getFailed().get(0).getMessage());
        else
            assertEquals(DatabaseWriter.UPDATE_ERROR_NOT_EXISTS, responseCollection.getFailed().get(0).getMessage());

        //Transactional
        mfevent.setTransaction(true);
        response = invokeLambda(mfevent.serialize());

        errorResponse = XyzSerializable.deserialize(response);
        assertEquals(XyzError.CONFLICT, errorResponse.getError());
        failedList = (ArrayList) errorResponse.getErrorDetails().get("FailedList");
        assertEquals(1, failedList.size());

        failure1 = (HashMap<String, String>) failedList.get(0);
        assertEquals("doesnotexist", failure1.get("id"));

        if (withConflictDetection)
            assertEquals(DatabaseWriter.UPDATE_ERROR_CONCURRENCY, failure1.get("message"));
        else
            assertEquals(DatabaseWriter.UPDATE_ERROR_NOT_EXISTS, failure1.get("message"));
    }
}
