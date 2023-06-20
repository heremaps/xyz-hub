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

package com.here.naksha.lib.psql;

import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.handler.psql.PsqlHandlerParams;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.naksha.lib.core.util.Hasher;
import com.here.naksha.lib.psql.tools.FeatureGenerator;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unused")
public class PSQLHashedSpaceIdIT extends PSQLAbstractIT {

    protected static Map<String, Object> connectorParams = new HashMap<String, Object>() {
        {
            put(PsqlHandlerParams.ENABLE_HASHED_SPACEID, true);
            put(PsqlHandlerParams.AUTO_INDEXING, true);
        }
    };

    @BeforeAll
    public static void init() throws Exception {
        initEnv(connectorParams);
    }

    @AfterAll
    public void shutdown() throws Exception {
        invokeDeleteTestSpace(connectorParams);
    }

    @Test
    public void testTableCreation() throws Exception {
        final String spaceId = "foo";
        final String hashedSpaceId = Hasher.getHash(spaceId);
        final XyzNamespace xyzNamespace = new XyzNamespace().withSpace("foo").withCreatedAt(1517504700726L);

        final List<Feature> features = new ArrayList<Feature>() {
            {
                add(FeatureGenerator.generateFeature(xyzNamespace, null));
            }
        };

        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
        // mfevent.setSpaceId(spaceId);
        // mfevent.setConnectorParams(connectorParams);
        mfevent.setTransaction(true);
        mfevent.setInsertFeatures(features);
        invokeLambda(mfevent.serialize());

        /** Needed to trigger update on pg_stat */
        try (final Connection connection = dataSource().getConnection();
                final Statement stmt = connection.createStatement();
                final ResultSet rs =
                        stmt.executeQuery("SELECT table_name FROM information_schema.tables WHERE table_name='"
                                + hashedSpaceId
                                + "'"); ) {
            assertTrue(rs.next());
            assertEquals(hashedSpaceId, rs.getString("table_name"));
        }
    }

    @Test
    public void testAutoIndexing() throws Exception {
        final String spaceId = "foo";
        final String hashedSpaceId = Hasher.getHash(spaceId);

        final List<Feature> features =
                FeatureGenerator.get11kFeatureCollection().getFeatures();
        ModifyFeaturesEvent mfevent = new ModifyFeaturesEvent();
        // mfevent.setSpaceId(spaceId);
        // mfevent.setConnectorParams(connectorParams);
        mfevent.setTransaction(true);
        mfevent.setInsertFeatures(features);

        invokeLambda(mfevent.serialize());

        /** Needed to trigger update on pg_stat */
        try (final Connection connection = dataSource().getConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("DELETE FROM xyz_config.xyz_idxs_status WHERE spaceid='" + hashedSpaceId + "';");
            stmt.execute("ANALYZE \"" + hashedSpaceId + "\";");
        }

        // Triggers dbMaintenance
        invokeLambdaFromFile("/events/HealthCheckWithEnableHashedSpaceIdEvent.json");

        try (final Connection connection = dataSource().getConnection()) {
            Statement stmt = connection.createStatement();
            // check for the index status
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT * FROM xyz_config.xyz_idxs_status where spaceid = '" + hashedSpaceId + "';")) {
                assertTrue(rs.next());
                assertTrue(rs.getBoolean("idx_creation_finished"));
                assertEquals(11_000L, rs.getInt("count"));
            }

            // check for the indexes itself
            try (ResultSet rs =
                    stmt.executeQuery("SELECT * FROM pg_indexes WHERE tablename = '" + hashedSpaceId + "';")) {
                final Set<String> indexes = new HashSet<String>() {
                    {
                        add("idx_" + hashedSpaceId + "_createdAt");
                        add("idx_" + hashedSpaceId + "_geo");
                        add("idx_" + hashedSpaceId + "_serial");
                        add("idx_" + hashedSpaceId + "_tags");
                        add("idx_" + hashedSpaceId + "_updatedAt");
                        add("idx_" + hashedSpaceId + "_id");
                    }
                };

                indexes.addAll(features.get(0).getProperties().additionalProperties().keySet().stream()
                        .filter(k -> !"test".equals(k))
                        .filter(k -> !"@ns:com:here:xyz".equals(k))
                        .map(k -> "idx_" + hashedSpaceId + "_"
                                + DigestUtils.md5Hex(k).substring(0, 7) + "_a")
                        .collect(Collectors.toSet()));

                final List<String> extractedIndexes = new ArrayList<>();
                while (rs.next()) {
                    extractedIndexes.add(rs.getString("indexname"));
                }

                assertTrue(extractedIndexes.containsAll(indexes));
                assertFalse(extractedIndexes.contains("idx_" + hashedSpaceId + "_"
                        + DigestUtils.md5Hex("test").substring(0, 7) + "_a"));
            }

            /* Clean-up maintenance entry */
            stmt.execute("DELETE FROM xyz_config.xyz_idxs_status WHERE spaceid='" + hashedSpaceId + "';");
        }
    }
}
