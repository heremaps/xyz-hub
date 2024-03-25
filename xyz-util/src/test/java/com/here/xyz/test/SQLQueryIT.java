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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

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

  private static String getQueryTextByQueryId(SQLQuery longRunningQuery, DataSourceProvider dsp) throws SQLException {
    return new SQLQuery("SELECT query FROM pg_stat_activity "
        + "WHERE state = 'active' "
        + "AND strpos(query, #{labelValue}) > 0 "
        + "AND pid != pg_backend_pid()")
        .withNamedParameter("labelValue", longRunningQuery.getQueryId())
        .run(dsp, rs -> rs.next() ? rs.getString("query") : null);
  }

  @Test
  public void startAndKillQuery() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery longRunningQuery = new SQLQuery("SELECT pg_sleep(30)");

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
      assertEquals(longRunningQuery.text(), getQueryTextByQueryId(longRunningQuery, dsp));

      //Kill the long-running query
      longRunningQuery.kill();

      //Check that the original query is not running anymore
      assertNull(getQueryTextByQueryId(longRunningQuery, dsp));
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
      assertNotNull(getQueryTextByQueryId(longRunningAsyncQuery, dsp));
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
    return new SQLQuery("CREATE TABLE SQLQueryIT (col TEXT);").write(dsp);
  }

  @Test
  public void runAsyncQueryWithRecursion() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        createTmpTable(dsp);

        new SQLQuery("""
            CREATE OR REPLACE FUNCTION test_func(value TEXT, depth INTEGER) RETURNS VOID AS
            $BODY$
            BEGIN
                IF depth = 10 THEN
                  INSERT INTO SQLQueryIT VALUES ('' || depth);
                ELSE
                  PERFORM asyncify($R$SELECT test_func('$R$ || value || $R$', $R$ || depth + 1 || $R$)$R$);
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

        Thread.sleep(200);

        assertEquals("10", new SQLQuery("SELECT col FROM SQLQueryIT")
            .run(dsp, rs -> rs.next() ? rs.getString("col") : null));
      }
      finally {
        dropTmpTable(dsp);
      }
    }
  }
}
