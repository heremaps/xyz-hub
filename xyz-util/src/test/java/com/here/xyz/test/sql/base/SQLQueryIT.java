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

package com.here.xyz.test.sql.base;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

public class SQLQueryIT extends SQLITBase {

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
  public void areRunningCheckMultipleValuesForSameLabel() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery longRunningQuery1 = new SQLQuery("SELECT pg_sleep(500)")
          .withLabel("taskId", "1");
      SQLQuery longRunningQuery2 = new SQLQuery("SELECT pg_sleep(500)")
          .withLabel("taskId", "2");

      new Thread(() -> {
        try {
          longRunningQuery1.run(dsp);
        }
        catch (SQLException e) {
          //Ignore
        }
      }).start();

      new Thread(() -> {
        try {
          longRunningQuery2.run(dsp);
        }
        catch (SQLException e) {
          //Ignore
        }
      }).start();

      //Wait until both queries are visible in pg_stat_activity.
      long deadline = System.currentTimeMillis() + 5_000;
      while (System.currentTimeMillis() < deadline && SQLQuery.areRunning(dsp, false, "taskId", Set.of("1", "2")).size() < 2)
        Thread.sleep(100);

      //Check that both taskIds are currently running
      assertEquals(Set.of(
              "1",
              "2"
      ), SQLQuery.areRunning(dsp, false, "taskId", Set.of(
              "1",
              "2"
      )));

      assertEquals(Set.of(
              "1"
      ), SQLQuery.areRunning(dsp, false, "taskId", Set.of(
              "1"
      )));

      assertEquals(Set.of(
              "2"
      ), SQLQuery.areRunning(dsp, false, "taskId", Set.of(
              "2"
      )));

      //Kill the long-running queries
      longRunningQuery1.kill();
      longRunningQuery2.kill();

      //Check that the original queries are not running anymore
      assertFalse(SQLQuery.isRunning(dsp, false, longRunningQuery1.getQueryId()));
      assertFalse(SQLQuery.isRunning(dsp, false, longRunningQuery2.getQueryId()));
      assertEquals(Set.of(), SQLQuery.areRunning(dsp, false, "taskId", Set.of(
             "1","2")));
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
  public void runAsyncQueryAndCheckIfIdleConnectionIsClearedInErrorCase() throws Exception {
    //ERROR
    SQLQuery asyncQuery = new SQLQuery("SELECT 1/0").withAsync(true);

    //Start the query and directly close the connection
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      asyncQuery.run(dsp);
    }

    //No Idle connection should be present
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      while (SQLQuery.isRunning(dsp, false, asyncQuery.getQueryId()))
        Thread.sleep(50);
      assertFalse(connectionIsIdle(dsp, asyncQuery.getQueryId()));
    }
  }

  @Test
  public void runAsyncQueryAndCheckIfIdleConnectionIsClearedInSuccessCase() throws Exception {
    //SUCCESS
    SQLQuery asyncQuery = new SQLQuery("SELECT 1").withAsync(true);

    //Start the query and directly close the connection
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      asyncQuery.run(dsp);
    }

    //No Idle connection should be present
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      while (SQLQuery.isRunning(dsp, false, asyncQuery.getQueryId()))
        Thread.sleep(50);
      assertFalse(connectionIsIdle(dsp, asyncQuery.getQueryId()));
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

        new SQLQuery("INSERT INTO ${tableName} VALUES (#{param});")
            .withVariable("tableName", getDefaultTmpTableName())
            .withNamedParameter("param", fancyString)
            .withAsync(true)
            .write(dsp);

        Thread.sleep(1_000);

        assertEquals(fancyString, new SQLQuery("SELECT col FROM ${tableName}")
            .withVariable("tableName", getDefaultTmpTableName())
            .run(dsp, rs -> rs.next() ? rs.getString("col") : null));
      }
      finally {
        //Delete the test table again
        dropTmpTable(dsp);
      }
    }
  }

  @Test
  public void runAsyncQueryWithRecursion() throws Exception {
    int waitTime = 0;
    int chainCount = 2;
    int chainLength = 10;

    for (int i = 0; i < chainCount; i++)
      startThreadChain("chain_" + i, waitTime, chainLength);

    Thread.sleep(chainLength * waitTime * 1_000 + 500);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        for (int i = 0; i < chainCount; i++)
          assertTmpTableSize(dsp, "chain_" + i, chainLength);
      }
      finally {
        for (int i = 0; i < chainCount; i++)
          dropTmpTable(dsp, "chain_" + i);
      }
    }
  }

  private void startThreadChain(String tableName, int waitTime, int chainLength) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp, tableName);
        //Prepare an empty test table
        createTmpTable(dsp, tableName);

        new SQLQuery("""
            CREATE OR REPLACE FUNCTION test_func_${{tableName}}(value TEXT, depth INTEGER) RETURNS VOID AS
            $BODY$
            DECLARE
              v INT;
            BEGIN
                SELECT coalesce(max(col::int), 0) FROM ${tableName} INTO v;
                RAISE NOTICE '############## Table: ${tableName}, previous value: %, labels: %', v, get_query_labels();
                --RAISE NOTICE '** %', 'TEST';
                INSERT INTO ${tableName} VALUES ('' || depth);
                
                PERFORM pg_sleep(${{waitTime}});
                
                
                IF depth < ${{chainLength}} THEN
                  PERFORM asyncify(format('SELECT test_func_${{tableName}}(%L , %s)', value, depth + 1));
                END IF;
                
                
            END
            $BODY$
            LANGUAGE plpgsql VOLATILE;
            """)
            .withVariable("tableName", tableName)
            .withQueryFragment("tableName", tableName)
            .withQueryFragment("waitTime", "" + waitTime)
            .withQueryFragment("chainLength", "" + chainLength)
            .write(dsp);

        new SQLQuery("SELECT test_func_${{tableName}}(#{value}, #{depth})")
            .withQueryFragment("tableName", tableName)
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
  }

  @Test
  public void runBatchWriteQuery() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp);

        SQLQuery tableCreationQuery = buildTableCreationQuery();
        SQLQuery insertQuery = new SQLQuery("INSERT INTO ${tableName} VALUES ('test')")
            .withVariable("tableName", getDefaultTmpTableName());

        SQLQuery.batchOf(tableCreationQuery, insertQuery).writeBatch(dsp);

        assertEquals("test", new SQLQuery("SELECT col FROM ${tableName}")
            .withVariable("tableName", getDefaultTmpTableName())
            .run(dsp, rs -> rs.next() ? rs.getString("col") : null));
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

        SQLQuery insertQuery = new SQLQuery("INSERT INTO ${tableName} VALUES ('test')")
            .withVariable("tableName", getDefaultTmpTableName());

        SQLQuery.batchOf(insertQuery).writeBatch(dsp);

        assertEquals("test", new SQLQuery("SELECT col FROM ${tableName}")
            .withVariable("tableName", getDefaultTmpTableName())
            .run(dsp, rs -> rs.next() ? rs.getString("col") : null));
      }
      finally {
        dropTmpTable(dsp);
      }
    }
  }

  @Test
  public void runQueryWithContext() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String key = "someKey";
      String value = "someValue";

      SQLQuery query = new SQLQuery("SELECT context()->>#{key};")
          .withContext(Map.of(key, value))
          .withNamedParameter("key", key);

      assertEquals(value, query.run(dsp, rs -> rs.next() ? rs.getString(1) : null));
    }
  }
}
