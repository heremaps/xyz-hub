/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.models.hub.Space;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;

public class PSQLDecoupledTableNameIT extends PSQLAbstractIT {

  private static final String SPACE_ID = "recreated-space";
  private static final String FIRST_TABLE = "firsttable";
  private static final String SECOND_TABLE = "secondtable";
  private static final Map<String, Object> CONNECTOR_PARAMS = Map.of(
      CONNECTOR_ID, "test-connector",
      ENABLE_HASHED_SPACEID, true);

  @BeforeClass
  public static void init() throws Exception {
    initEnv(CONNECTOR_PARAMS);
  }

  @After
  public void cleanUp() throws Exception {
    deletePhysicalTable(FIRST_TABLE);
    deletePhysicalTable(SECOND_TABLE);
  }

  @Test
  public void createsTwoPhysicalTablesForTheSameLogicalSpace() throws Exception {
    createPhysicalTable(FIRST_TABLE);
    createPhysicalTable(SECOND_TABLE);

    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        Statement statement = connection.createStatement();
        ResultSet tables = statement.executeQuery("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND table_name IN ('firsttable', 'secondtable')
            """)) {
      int tableCount = 0;
      while (tables.next())
        tableCount++;
      assertEquals(2, tableCount);
    }

    try (Connection connection = LAMBDA.dataSourceProvider.getWriter().getConnection();
        Statement statement = connection.createStatement();
        ResultSet metadata = statement.executeQuery(
            "SELECT h_id FROM xyz_config.space_meta WHERE id = 'recreated-space' AND schem = 'public'")) {
      assertTrue(metadata.next());
      assertEquals(SECOND_TABLE, metadata.getString("h_id"));
    }
  }

  private static void createPhysicalTable(String tableName) throws Exception {
    ModifySpaceEvent event = new ModifySpaceEvent()
        .withSpace(SPACE_ID)
        .withOperation(ModifySpaceEvent.Operation.CREATE)
        .withSpaceDefinition(new Space().withId(SPACE_ID))
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(Map.of(TABLE_NAME, tableName));
    invokeLambda(event);
  }

  private static void deletePhysicalTable(String tableName) throws Exception {
    ModifySpaceEvent event = new ModifySpaceEvent()
        .withSpace(SPACE_ID)
        .withOperation(ModifySpaceEvent.Operation.DELETE)
        .withConnectorParams(CONNECTOR_PARAMS)
        .withParams(Map.of(TABLE_NAME, tableName));
    invokeLambda(event);
  }
}
