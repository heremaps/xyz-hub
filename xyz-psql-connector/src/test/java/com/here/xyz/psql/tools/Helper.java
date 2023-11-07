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

package com.here.xyz.psql.tools;

import static io.restassured.path.json.JsonPath.with;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzResponse;
import java.util.List;

public class Helper {

    protected static <T extends XyzResponse> T deserializeResponse(String jsonResponse) throws JsonProcessingException, ErrorResponseException {
      XyzResponse response = XyzSerializable.deserialize(jsonResponse);
      if (response instanceof ErrorResponse)
        throw new ErrorResponseException(((ErrorResponse) response).getError(), ((ErrorResponse) response).getErrorMessage());
      return (T) response;
    }

    protected void assertNoErrorInResponse(String response) {
        assertNull(with(response).get("$.errorMessage"));
        assertNull(with(response).get("$.error"));
    }

    protected void assertNoErrorInResponse(XyzResponse response) {
        boolean isError = response instanceof ErrorResponse;
        if (isError) {
            ErrorResponse err = (ErrorResponse) response;
            assertFalse("ErrorResponse[" + err.getError() + "]: " + err.getErrorMessage(), isError);
        }
    }

    protected void assertRead(String insertRequest, String response, boolean checkVersion) throws Exception {
        final List<Feature> responseFeatures = ((FeatureCollection) deserializeResponse(response)).getFeatures();
        final ModifyFeaturesEvent mfe = XyzSerializable.deserialize(insertRequest);
        String space = mfe.getSpace();
        List<Feature> requestFeatures = mfe.getInsertFeatures();
        if (requestFeatures == null)
            return;

        for (int i = 0; i < requestFeatures.size(); i++) {
            Feature requestFeature = requestFeatures.get(i);
            Feature responseFeature = responseFeatures.get(i);
            assertTrue("Check geometry", jsonCompare(requestFeature.getGeometry(), responseFeature.getGeometry()));
            assertEquals("Check name", (String) requestFeature.getProperties().get("name"), responseFeature.getProperties().get("name"));
            assertNotNull("Check id", responseFeature.getId());
            assertTrue("Check tags", jsonCompare(requestFeature.getProperties().getXyzNamespace().getTags(),
                    responseFeature.getProperties().getXyzNamespace().getTags()));
            assertEquals("Check space", space, responseFeature.getProperties().getXyzNamespace().getSpace());
            assertNotEquals("Check createdAt", 0L, responseFeature.getProperties().getXyzNamespace().getCreatedAt());
            assertNotEquals("Check updatedAt", 0L, responseFeature.getProperties().getXyzNamespace().getUpdatedAt());

            assertNotEquals("Check version", -1, responseFeature.getProperties().getXyzNamespace().getVersion());
        }
    }

    protected boolean jsonCompare(Object o1, Object o2) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode tree1 = mapper.convertValue(o1, JsonNode.class);
        JsonNode tree2 = mapper.convertValue(o2, JsonNode.class);
        return tree1.equals(tree2);
    }
}
