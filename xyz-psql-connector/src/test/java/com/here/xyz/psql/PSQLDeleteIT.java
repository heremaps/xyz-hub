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

import com.amazonaws.util.IOUtils;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PSQLDeleteIT extends PSQLAbstractIT {

    @BeforeClass
    public static void init() throws Exception { initEnv(null); }

    @Before
    public void createTable() throws Exception {
        invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    }

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
}