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

import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.lib.core.models.Typed;
import com.here.naksha.lib.core.models.geojson.implementation.FeatureCollection;
import com.here.naksha.lib.core.models.payload.events.feature.IterateFeaturesEvent;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.XyzError;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unused")
public class PSQLResponseSizeIT extends PSQLAbstractIT {

    static Map<String, Object> connectorParams = new HashMap<String, Object>() {
        {
        }
    };

    @BeforeAll
    public static void init() throws Exception {
        initEnv(connectorParams);
    }

    @BeforeAll
    public void prepare() throws Exception {
        invokeDeleteTestSpace(connectorParams);
        invokeLambdaFromFile("/events/InsertFeaturesEventTransactional.json");
    }

    @AfterAll
    public void shutdown() throws Exception {
        invokeDeleteTestSpace(connectorParams);
    }

    @Test
    public void testMaxConnectorResponseSize() throws Exception {
        final IterateFeaturesEvent iter = new IterateFeaturesEvent();
        // iter.setSpaceId("foo");
        // iter.setConnectorParams(connectorParams);

        // connectorParams.put(AbstractConnectorHandler.MAX_UNCOMPRESSED_RESPONSE_SIZE, 1024);
        Typed result = JsonSerializable.deserialize(invokeLambda(iter.serialize()));
        assertTrue(result instanceof FeatureCollection);

        // connectorParams.put(AbstractConnectorHandler.MAX_UNCOMPRESSED_RESPONSE_SIZE, 512);
        result = JsonSerializable.deserialize(invokeLambda(iter.serialize()));
        assertTrue(result instanceof ErrorResponse);
        assertEquals(((ErrorResponse) result).getError(), XyzError.PAYLOAD_TO_LARGE);

        // connectorParams.put(AbstractConnectorHandler.MAX_UNCOMPRESSED_RESPONSE_SIZE, 1);
        result = JsonSerializable.deserialize(invokeLambda(iter.serialize()));
        assertTrue(result instanceof ErrorResponse);
        assertEquals(((ErrorResponse) result).getError(), XyzError.PAYLOAD_TO_LARGE);

        // connectorParams.put(AbstractConnectorHandler.MAX_UNCOMPRESSED_RESPONSE_SIZE, 0);
        result = JsonSerializable.deserialize(invokeLambda(iter.serialize()));
        assertTrue(result instanceof FeatureCollection);
    }
}
