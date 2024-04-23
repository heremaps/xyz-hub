/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

package com.here.xyz.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.here.xyz.util.db.DatabaseSettings;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.PooledDataSources;
import java.sql.SQLException;
import org.junit.Test;

public class SQLQueryIT {
  protected static final String PG_HOST = "localhost";
  protected static final String PG_DB = "postgres";
  protected static final String PG_USER = "postgres";
  protected static final String PG_PW = "password";

  private static DataSourceProvider getDataSourceProvider() {
    return new PooledDataSources(new DatabaseSettings("testPSQL")
        .withHost(PG_HOST)
        .withDb(PG_DB)
        .withUser(PG_USER)
        .withPassword(PG_PW)
        .withDbMaxPoolSize(2));
  }

  @Test
  public void startAndKillQuery() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery longRunningQuery = new SQLQuery("SELECT pg_sleep(500)");

      new Thread(() -> {
        try {
          longRunningQuery.run(dsp);
        }
        catch (SQLException e) {
          //Ignore
        }
      }).start();

      //Wait some time to make sure query execution actually gets started on the DB
      try {
        Thread.sleep(1_000);
      }
      catch (InterruptedException e) {
        //Ignore
      }

      //Check that the long-running query is actually running
      assertTrue(SQLQuery.isRunning(dsp, false, longRunningQuery.getQueryId()));

      //Kill the long-running query
      longRunningQuery.kill();

      //Check that the original query is not running anymore
      assertFalse(SQLQuery.isRunning(dsp, false, longRunningQuery.getQueryId()));
    }
  }

  @Test
  public void runAsyncQuery() throws Exception {
    SQLQuery longRunningAsyncQuery = new SQLQuery("SELECT pg_sleep(10)").withAsync(true);

    //Start the query and directly close the connection
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      longRunningAsyncQuery.run(dsp);
    }

    Thread.sleep(1_000);

    //The query should still be running on the database
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      assertTrue(SQLQuery.isRunning(dsp, false, longRunningAsyncQuery.getQueryId()));
    }
  }

  @Test
  public void runAsyncQueryWithParameter() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp);
        //Prepare an empty test table
        createTmpTable(dsp);

        //Run an async query with parameter
        final String fancyString = """
            so''meF'ancy"String
            """;

        new SQLQuery("INSERT INTO SQLQueryIT VALUES (#{param});").withNamedParameter("param", fancyString).withAsync(true).write(dsp);

        Thread.sleep(1_000);

        assertEquals(fancyString, new SQLQuery("SELECT col FROM SQLQueryIT")
            .run(dsp, rs -> rs.next() ? rs.getString("col") : null));
      }
      finally {
        //Delete the test table again
        dropTmpTable(dsp);
      }
    }
  }

  private static int dropTmpTable(DataSourceProvider dsp) throws SQLException {
    return new SQLQuery("DROP TABLE IF EXISTS SQLQueryIT;").write(dsp);
  }

  private static int createTmpTable(DataSourceProvider dsp) throws SQLException {
    return buildTableCreationQuery().write(dsp);
  }

  private static SQLQuery buildTableCreationQuery() {
    return new SQLQuery("CREATE TABLE SQLQueryIT (col TEXT);");
  }

  @Test
  public void runAsyncQueryWithRecursion() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp);
        //Prepare an empty test table
        createTmpTable(dsp);

        new SQLQuery("""
            CREATE OR REPLACE FUNCTION test_func(value TEXT, depth INTEGER) RETURNS VOID AS
            $BODY$
            DECLARE
              v INT;
            BEGIN
                SELECT coalesce(max(col::int), 0) FROM SQLQueryIT INTO v;
                RAISE NOTICE 'previous value: %, labels: %', v, get_query_labels();
                INSERT INTO SQLQueryIT VALUES ('' || depth);
                
                IF depth < 10 THEN
                  PERFORM asyncify(format('SELECT test_func(%L , %s)', value, depth + 1));
                END IF;
            END
            $BODY$
            LANGUAGE plpgsql VOLATILE;
            """).write(dsp);

        new SQLQuery("SELECT test_func(#{value}, #{depth})")
            .withNamedParameter("value", "testValue")
            .withNamedParameter("depth", 1)
            .withAsync(true)
            .run(dsp);
      }
      catch (Exception e) {
        dropTmpTable(dsp);
        throw e;
      }
    }

    Thread.sleep(200);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        assertEquals(10, (int) new SQLQuery("SELECT count(1) as count FROM SQLQueryIT")
            .run(dsp, rs -> rs.next() ? rs.getInt("count") : null));
      }
      finally {
        dropTmpTable(dsp);
      }
    }

  }

  @Test
  public void runBatchWriteQuery() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp);

        SQLQuery tableCreationQuery = buildTableCreationQuery();
        SQLQuery insertQuery = new SQLQuery("INSERT INTO SQLQueryIT VALUES ('test')");

        SQLQuery.batchOf(tableCreationQuery, insertQuery).writeBatch(dsp);

        assertEquals("test", new SQLQuery("SELECT col FROM SQLQueryIT").run(dsp, rs -> rs.next() ? rs.getString("col") : null));
      }
      finally {
        dropTmpTable(dsp);
      }
    }
  }

  @Test
  public void runBatchWriteQueryOfSizeOne() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp);
        createTmpTable(dsp);

        SQLQuery insertQuery = new SQLQuery("INSERT INTO SQLQueryIT VALUES ('test')");

        SQLQuery.batchOf(insertQuery).writeBatch(dsp);

        assertEquals("test", new SQLQuery("SELECT col FROM SQLQueryIT").run(dsp, rs -> rs.next() ? rs.getString("col") : null));
      }
      finally {
        dropTmpTable(dsp);
      }
    }
  }
}
