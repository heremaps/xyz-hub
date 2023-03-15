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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.here.mapcreator.ext.naksha.NPsqlConnectorParams;
import com.here.xyz.Typed;
import com.here.xyz.XyzSerializable;
import com.here.xyz.connectors.AbstractConnectorHandler;
import com.here.xyz.events.IterateFeaturesEvent;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import java.util.HashMap;
import java.util.Map;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

@SuppressWarnings("unused")
public class PSQLResponseSizeIT extends PSQLAbstractIT {

  static Map<String, Object> connectorParams = new HashMap<String,Object>(){{
    put(NPsqlConnectorParams.CONNECTOR_ID, "test-connector");
  }};

  @BeforeClass
  public static void init() throws Exception { initEnv(connectorParams); }

  @Before
  public void prepare() throws Exception {
    invokeDeleteTestSpace(connectorParams);
    invokeLambdaFromFile("/events/InsertFeaturesEventTransactional.json");
  }

  @After
  public void shutdown() throws Exception { invokeDeleteTestSpace(connectorParams); }

  @Test
  public void testMaxConnectorResponseSize() throws Exception {
    final IterateFeaturesEvent iter = new IterateFeaturesEvent()
        .withSpace("foo")
        .withConnectorParams(connectorParams);

    connectorParams.put(AbstractConnectorHandler.MAX_UNCOMPRESSED_RESPONSE_SIZE, 1024);
    Typed result = XyzSerializable.deserialize(invokeLambda(iter.serialize()));
    assertTrue(result instanceof FeatureCollection);

    connectorParams.put(AbstractConnectorHandler.MAX_UNCOMPRESSED_RESPONSE_SIZE, 512);
    result = XyzSerializable.deserialize(invokeLambda(iter.serialize()));
    assertTrue(result instanceof ErrorResponse);
    assertEquals(((ErrorResponse) result).getError(), XyzError.PAYLOAD_TO_LARGE);

    connectorParams.put(AbstractConnectorHandler.MAX_UNCOMPRESSED_RESPONSE_SIZE, 1);
    result = XyzSerializable.deserialize(invokeLambda(iter.serialize()));
    assertTrue(result instanceof ErrorResponse);
    assertEquals(((ErrorResponse) result).getError(), XyzError.PAYLOAD_TO_LARGE);

    connectorParams.put(AbstractConnectorHandler.MAX_UNCOMPRESSED_RESPONSE_SIZE, 0);
    result = XyzSerializable.deserialize(invokeLambda(iter.serialize()));
    assertTrue(result instanceof FeatureCollection);
  }
}
