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

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.LoadFeaturesEvent;
import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzResponse;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class PSQLLoadFeatures extends PSQLAbstractIT {

  @BeforeClass
  public static void init() throws Exception {
    initEnv(null);
  }

  @Before
  public void createTable() throws Exception {
    invokeCreateTestSpace(defaultTestConnectorParams, TEST_SPACE_ID);
    writeFeatures();
  }

  @After
  public void shutdown() throws Exception {
    invokeDeleteTestSpace(null);
  }

  public void writeFeatures() throws Exception {
    XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);

    ModifyFeaturesEvent mfe = new ModifyFeaturesEvent()
            .withConnectorParams(defaultTestConnectorParams)
            .withSpace("foo")
            .withTransaction(true)
            .withEnableUUID(true)
            .withVersionsToKeep(10)
            .withInsertFeatures(Arrays.asList(
                    new Feature().withId("F1").withProperties(new Properties().withXyzNamespace(xyzNamespace)),
                    new Feature().withId("F2").withProperties(new Properties().withXyzNamespace(xyzNamespace))
            ));
    invokeLambda(mfe.serialize());
  }

  @Test
  public void testLoadFeatures() throws Exception {
    LoadFeaturesEvent event = new LoadFeaturesEvent()
        .withConnectorParams(defaultTestConnectorParams)
        .withSpace("foo")
        .withVersionsToKeep(10)
        .withIdsMap(new HashMap<String, String>() {{
          put("F1", "0");
          put("F2", "0");
        }});

    FeatureCollection loadedFeatures = deserializeResponse(invokeLambda(event.serialize()));
    assertEquals(2, loadedFeatures.getFeatures().size());
    assertTrue(loadedFeatures.getFeatures().stream().anyMatch(f -> "F1".equals(f.getId())));
    assertTrue(loadedFeatures.getFeatures().stream().anyMatch(f -> "F2".equals(f.getId())));
    assertTrue(loadedFeatures.getFeatures().stream().allMatch(f -> f.getProperties().getXyzNamespace().getVersion() == 0));
  }
}
