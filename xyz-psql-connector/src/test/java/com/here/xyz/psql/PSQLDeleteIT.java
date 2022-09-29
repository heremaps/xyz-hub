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
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class PSQLDeleteIT extends PSQLAbstractIT {

    @BeforeClass
    public static void init() throws Exception { initEnv(null); }

    @After
    public void shutdown() throws Exception { invokeDeleteTestSpace(null); }

    @Test
    public void testDeleteFeatures() throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEvent.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        LOGGER.info("RAW RESPONSE: " + insertResponse);

        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, false);

        final JsonPath jsonPathFeatureIds = JsonPath.compile("$.features..id");
        List<String> ids = jsonPathFeatureIds.read(insertResponse, jsonPathConf);
        LOGGER.info("Preparation: Inserted features {}", ids);

        // =========== DELETE ==========
        final DocumentContext modifyFeaturesEventDoc = getEventFromResource("/events/InsertFeaturesEvent.json");
        modifyFeaturesEventDoc.delete("$.insertFeatures");

        Map<String, String> idsMap = new HashMap<>();
        ids.forEach(id -> idsMap.put(id, null));
        modifyFeaturesEventDoc.put("$", "deleteFeatures", idsMap);

        String deleteEvent = modifyFeaturesEventDoc.jsonString();
        String deleteResponse = invokeLambda(deleteEvent);
        assertNoErrorInResponse(deleteResponse);
        LOGGER.info("Modify features tested successfully");
    }

    @Test
    public void testDeleteFeaturesByTagWithOldStates() throws Exception {
        testDeleteFeaturesByTag(true);
    }

    @Test
    public void testDeleteFeaturesByTagDefault() throws Exception {
        testDeleteFeaturesByTag(false);
    }

    protected void testDeleteFeaturesByTag(boolean includeOldStates) throws Exception {
        // =========== INSERT ==========
        String insertJsonFile = "/events/InsertFeaturesEvent.json";
        String insertResponse = invokeLambdaFromFile(insertJsonFile);
        LOGGER.info("RAW RESPONSE: " + insertResponse);
        String insertRequest = IOUtils.toString(this.getClass().getResourceAsStream(insertJsonFile));
        assertRead(insertRequest, insertResponse, false);
        LOGGER.info("Preparation: Insert features");

        // =========== COUNT ==========
        String countResponse = invokeLambdaFromFile("/events/CountFeaturesEvent.json");
        Integer originalCount = JsonPath.read(countResponse, "$.count");
        LOGGER.info("Preparation: feature count = {}", originalCount);

        // =========== DELETE SOME TAGGED FEATURES ==========
        LOGGER.info("Delete tagged features");
        final DocumentContext deleteByTagEventDoc = getEventFromResource("/events/DeleteFeaturesByTagEvent.json");
        deleteByTagEventDoc.put("$", "params", Collections.singletonMap("includeOldStates", includeOldStates));
        String[][] tags = {{"yellow"}};
        deleteByTagEventDoc.put("$", "tags", tags);
        String deleteByTagEvent = deleteByTagEventDoc.jsonString();
        String deleteByTagResponse = invokeLambda(deleteByTagEvent);
        assertNoErrorInResponse(deleteByTagResponse);
        final JsonPath jsonPathFeatures = JsonPath.compile("$.features");
        @SuppressWarnings("rawtypes") List features = jsonPathFeatures.read(deleteByTagResponse, jsonPathConf);
        if (includeOldStates) {
            assertNotNull("'features' element in DeleteByTagResponse is missing", features);
            assertTrue("'features' element in DeleteByTagResponse is empty", features.size() > 0);
        } else if (features != null) {
            assertEquals("unexpected features in DeleteByTagResponse", 0, features.size());
        }

        countResponse = invokeLambdaFromFile("/events/CountFeaturesEvent.json");
        Integer count = JsonPath.read(countResponse, "$.count");
        assertTrue(originalCount > count);
        LOGGER.info("Delete tagged features tested successfully");

        // =========== DELETE ALL FEATURES ==========
        deleteByTagEventDoc.put("$", "tags", null);
        String deleteAllEvent = deleteByTagEventDoc.jsonString();
        String deleteAllResponse = invokeLambda(deleteAllEvent);
        assertNoErrorInResponse(deleteAllResponse);
        features = jsonPathFeatures.read(deleteAllResponse, jsonPathConf);
        if (features != null) {
            assertEquals("unexpected features in DeleteByTagResponse", 0, features.size());
        }

        countResponse = invokeLambdaFromFile("/events/CountFeaturesEvent.json");
        count = JsonPath.read(countResponse, "$.count");
        assertEquals(0, count.intValue());
        LOGGER.info("Delete all features tested successfully");
    }
}
