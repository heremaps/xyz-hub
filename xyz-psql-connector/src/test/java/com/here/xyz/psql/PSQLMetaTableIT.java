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

import com.here.xyz.models.hub.psql.PsqlProcessorParams;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class PSQLMetaTableIT extends PSQLAbstractIT {
    protected static Map<String, Object> connectorParams = new HashMap<String,Object>(){
        {   put(PsqlProcessorParams.ID, "test-connector");
            put(PsqlProcessorParams.ENABLE_HASHED_SPACEID, true);
        }
    };

    @BeforeClass
    public static void init() throws Exception { initEnv(connectorParams); }

    @After
    public void shutdown() throws Exception { invokeDeleteTestSpace(connectorParams); }

    @Test
    public void testMetaTableEntry() throws Exception {
        String q = "SELECT * FROM xyz_config.space_meta WHERE id='"+TEST_SPACE_ID+"';";

        invokeCreateTestSpace(connectorParams,TEST_SPACE_ID);

        try (final Connection connection = dataSource().getConnection()) {
            Statement stmt = connection.createStatement();
            ResultSet resultSet = stmt.executeQuery(q);

            /** Check Meta record */
            assertTrue(resultSet.next());
            assertEquals(TEST_SPACE_ID,resultSet.getString("id"));
            assertEquals("public",resultSet.getString("schem"));
            assertNotEquals(TEST_SPACE_ID,resultSet.getString("h_id"));
            assertEquals("{}",resultSet.getString("meta"));
        }
        // Delete Space
        invokeDeleteTestSpace(connectorParams);

        try (final Connection connection = dataSource().getConnection()) {
            Statement stmt = connection.createStatement();
            /** Check Meta if record is deleted*/
            ResultSet resultSet = stmt.executeQuery(q);
            assertFalse(resultSet.next());
        }
    }
}
