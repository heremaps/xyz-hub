/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import com.here.xyz.events.ModifyFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import com.here.xyz.psql.tools.FeatureGenerator;
import com.here.xyz.util.Hasher;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("unused")
public class PSQLHashedSpaceIdIT extends PSQLAbstractIT {

  protected static Map<String, Object> connectorParams = new HashMap<>(){
        {   put(PSQLAbstractIT.CONNECTOR_ID, "test-connector");
            put(PSQLAbstractIT.ENABLE_HASHED_SPACEID, true);
            put(PSQLAbstractIT.AUTO_INDEXING, true);
        }
  };

  @BeforeClass
  public static void init() throws Exception {
    initEnv(connectorParams);
  }

  @Before
  public void createTable() throws Exception {
    invokeCreateTestSpace(connectorParams,TEST_SPACE_ID);
  }

  @After
  public void shutdown() throws Exception { invokeDeleteTestSpace(connectorParams); }

  @Test
  public void testTableCreation() throws Exception {
    final String spaceId = "foo";
    final String hashedSpaceId = Hasher.getHash(spaceId);
    final XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);

    final List<Feature> features = new ArrayList<Feature>(){{
      add(FeatureGenerator.generateFeature(xyzNamespace, null));
    }};

    ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent()
            .withSpace(spaceId)
            .withConnectorParams(connectorParams)
            .withTransaction(true)
            .withInsertFeatures(features);
    invokeLambda(mfevent);

    /** Needed to trigger update on pg_stat */
    try (
            final Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
            final Statement stmt = connection.createStatement();
            final ResultSet rs = stmt.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_name='" + hashedSpaceId + "'");
    ) {
      assertTrue(rs.next());
      assertEquals(hashedSpaceId, rs.getString("table_name"));
    }
  }
}
