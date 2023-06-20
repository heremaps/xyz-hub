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

package com.here.xyz.psql.tools;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.xyz.util.json.JsonSerializable;
import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.DocumentContext;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.Option;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;

public class Helper {

    protected final Configuration jsonPathConf =
            Configuration.defaultConfiguration().addOptions(Option.SUPPRESS_EXCEPTIONS);

    protected void setPUUID(FeatureCollection featureCollection) throws JsonProcessingException {
        for (Feature feature : featureCollection.getFeatures()) {
            feature.getProperties()
                    .getXyzNamespace()
                    .setPuuid(feature.getProperties().getXyzNamespace().getUuid());
            feature.getProperties().getXyzNamespace().setUuid(UUID.randomUUID().toString());
        }
    }

    protected void assertNoErrorInResponse(String response) {
        assertNull(JsonPath.compile("$.error").read(response, jsonPathConf));
    }

    protected void assertRead(String insertRequest, String response, boolean checkGuid) throws Exception {
        final FeatureCollection responseCollection = JsonSerializable.deserialize(response);
        final List<Feature> responseFeatures = responseCollection.getFeatures();

        final ModifyFeaturesEvent gsModifyFeaturesEvent = JsonSerializable.deserialize(insertRequest);
        List<Feature> modifiedFeatures;

        modifiedFeatures = gsModifyFeaturesEvent.getInsertFeatures();
        assertReadFeatures(gsModifyFeaturesEvent.getSpaceId(), checkGuid, modifiedFeatures, responseFeatures);

        modifiedFeatures = gsModifyFeaturesEvent.getUpsertFeatures();
        assertReadFeatures(gsModifyFeaturesEvent.getSpaceId(), checkGuid, modifiedFeatures, responseFeatures);
    }

    protected void assertReadFeatures(
            String space, boolean checkGuid, List<Feature> requestFeatures, List<Feature> responseFeatures) {
        if (requestFeatures == null) {
            return;
        }
        for (int i = 0; i < requestFeatures.size(); i++) {
            Feature requestFeature = requestFeatures.get(i);
            Feature responseFeature = responseFeatures.get(i);
            assertTrue(jsonCompare(requestFeature.getGeometry(), responseFeature.getGeometry()));
            assertEquals(
                    (String) requestFeature.getProperties().get("name"),
                    responseFeature.getProperties().get("name"));
            assertNotNull(responseFeature.getId());
            assertTrue(jsonCompare(
                    requestFeature.getProperties().getXyzNamespace().getTags(),
                    responseFeature.getProperties().getXyzNamespace().getTags()));
            assertEquals(
                    space, responseFeature.getProperties().getXyzNamespace().getSpace());
            assertNotEquals(
                    0L, responseFeature.getProperties().getXyzNamespace().getCreatedAt());
            assertNotEquals(
                    0L, responseFeature.getProperties().getXyzNamespace().getUpdatedAt());
            assertNull(responseFeature.getProperties().getXyzNamespace().getPuuid());

            if (checkGuid) {
                assertNotNull(responseFeature.getProperties().getXyzNamespace().getUuid());
            } else {
                assertNull(responseFeature.getProperties().getXyzNamespace().getUuid());
            }
        }
    }

    protected boolean jsonCompare(Object o1, Object o2) {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode tree1 = mapper.convertValue(o1, JsonNode.class);
        JsonNode tree2 = mapper.convertValue(o2, JsonNode.class);
        return tree1.equals(tree2);
    }

    protected DocumentContext getEventFromResource(String file) {
        InputStream inputStream = this.getClass().getResourceAsStream(file);
        return JsonPath.parse(inputStream);
    }
}
