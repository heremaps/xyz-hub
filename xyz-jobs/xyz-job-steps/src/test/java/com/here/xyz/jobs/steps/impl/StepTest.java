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

package com.here.xyz.jobs.steps.impl;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.util.test.StepTestBase;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Space.ConnectorRef;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.sql.SQLException;

public abstract class StepTest extends StepTestBase {
  /** A local test connector that creates the tables of new spaces with their first write, on the database "postgres". */
  protected static final String LAZY_CONNECTOR = "psql-lazy";
  /** The same on the database "postgres_db2". */
  protected static final String LAZY_DB2_CONNECTOR = "psql-db2-lazy";
  private static final String PG_HOST = System.getProperty("pg.host", "localhost");
  protected static final String DB_URL = "jdbc:postgresql://" + PG_HOST + "/postgres";
  protected static final String DB2_URL = "jdbc:postgresql://" + PG_HOST + "/postgres_db2";
  /** Further spaces of the running test case, deleted after it in reverse order. */
  private final List<String> testSpaces = new ArrayList<>();

  @BeforeEach
  public void setup() throws SQLException {
    cleanup();
    createSpace(SPACE_ID);
  }

  @AfterEach
  public void cleanup() throws SQLException {
    for (int i = testSpaces.size() - 1; i >= 0; i--)
      deleteSpace(testSpaces.get(i));
    testSpaces.clear();
    deleteSpace(SPACE_ID);
    cleanS3Files(JOB_ID);
  }

  /**
   * Creates a further space, which is deleted after the test case.
   *
   * @return The created space as the Hub stores it
   */
  protected Space createTestSpace(Space space) {
    Space created = createSpace(space, false);
    assertNotNull(created, "The space " + space.getId() + " was not created");
    testSpaces.add(space.getId());
    return loadSpace(space.getId());
  }

  /**
   * Creates a further space on a connector that was just registered, as soon as the Hub has a client for that connector. The
   * Hub creates it in its config update cycle, which runs every 2 minutes; until then a create answers 502.
   *
   * @return The created space as the Hub stores it
   */
  protected Space createTestSpaceWhenConnectorIsReady(Space space, long timeoutMillis) throws InterruptedException {
    long deadline = System.currentTimeMillis() + timeoutMillis;
    while (createSpace(space, false) == null) {
      if (System.currentTimeMillis() > deadline)
        throw new AssertionError("The Hub had no client for connector " + space.getStorage().getId() + " after " + timeoutMillis
            + " ms");
      Thread.sleep(5_000);
    }
    testSpaces.add(space.getId());
    return loadSpace(space.getId());
  }

  /**
   * Creates a further space on a connector with lazy table creation, which is deleted after the test case.
   *
   * @return The created space, which has the lazyTable flag and no table
   */
  protected Space createLazyTestSpace(String spaceId, String connectorId) {
    Space space = createTestSpace(new Space().withId(spaceId).withStorage(new ConnectorRef().withId(connectorId)));
    assertTrue(isFlagged(space.getId()), "A new space of " + connectorId + " has the flag");
    return space;
  }

  /**
   * @return Whether the config of the space, read through the Hub, has the lazyTable flag
   */
  protected boolean isFlagged(String spaceId) {
    return Space.isLazyTable(loadSpace(spaceId).getStorage().getParams());
  }

  protected static String tableName(Space space) {
    return (String) space.getStorage().getParams().get(TABLE_NAME);
  }

  /**
   * @return The number of relations whose name starts with the table name: the table, its partitions, sequences and indexes
   */
  protected static int relationCount(String dbUrl, String table) throws SQLException {
    try (Connection connection = DriverManager.getConnection(dbUrl, "postgres", "password");
        PreparedStatement stmt = connection.prepareStatement("SELECT count(*) FROM pg_class WHERE starts_with(relname, ?)")) {
      stmt.setString(1, table);
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        return rs.getInt(1);
      }
    }
  }

  /**
   * @return Whether the table exists in the schema "public" of the database
   */
  protected static boolean tableExists(String dbUrl, String table) throws SQLException {
    try (Connection connection = DriverManager.getConnection(dbUrl, "postgres", "password");
        PreparedStatement stmt = connection.prepareStatement("SELECT to_regclass(?) IS NOT NULL")) {
      stmt.setString(1, "public.\"" + table.replace("\"", "\"\"") + "\"");
      try (ResultSet rs = stmt.executeQuery()) {
        rs.next();
        return rs.getBoolean(1);
      }
    }
  }
}
