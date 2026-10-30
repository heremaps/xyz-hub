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
import static org.junit.Assert.fail;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.datasource.PooledDataSources;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SQLITBase {
  protected static final String PG_HOST = "localhost";
  protected static final String PG_DB = "postgres";
  protected static final String PG_USER = "postgres";
  protected static final String PG_PW = "password";
  protected static final DatabaseSettings DB_SETTINGS = new DatabaseSettings("testPSQL")
      .withApplicationName(SQLITBase.class.getSimpleName())
      .withHost(PG_HOST)
      .withDb(PG_DB)
      .withUser(PG_USER)
      .withPassword(PG_PW)
      .withDbMaxPoolSize(2)
      .withScriptResourcePaths(List.of(new ScriptResourcePath("/sql", "hub", "common")));

  protected String getDefaultTmpTableName() {
    return getClass().getSimpleName();
  }

  //NOTE: The following are convenience methods to be used by subclasses

  protected static DataSourceProvider getDataSourceProvider() {
    return new PooledDataSources(DB_SETTINGS);
  }

  protected static DataSourceProvider getDataSourceProvider(DatabaseSettings dbSettings) {
    return new PooledDataSources(dbSettings);
  }

  protected static boolean connectionIsIdle(DataSourceProvider dsp, String queryId) throws SQLException {
    return new SQLQuery("""
        SELECT 1 FROM pg_stat_activity
          WHERE state = 'idle' AND query LIKE '%${{queryId}}%' AND pid != pg_backend_pid()
        """)
        .withQueryFragment("queryId", queryId)
        .withLoggingEnabled(false)
        .run(dsp, rs -> rs.next());
  }

  protected int dropTmpTable(DataSourceProvider dsp) throws SQLException {
    return dropTmpTable(dsp, getDefaultTmpTableName());
  }

  protected int dropTmpTable(DataSourceProvider dsp, String tableName) throws SQLException {
    return new SQLQuery("DROP TABLE IF EXISTS ${tableName};")
        .withVariable("tableName", tableName)
        .write(dsp);
  }

  protected int createTmpTable(DataSourceProvider dsp) throws SQLException {
    return createTmpTable(dsp, getDefaultTmpTableName());
  }

  protected int createTmpTable(DataSourceProvider dsp, String tableName) throws SQLException {
    return buildTableCreationQuery(tableName).write(dsp);
  }

  protected SQLQuery buildTableCreationQuery() {
    return buildTableCreationQuery(getDefaultTmpTableName());
  }

  private SQLQuery buildTableCreationQuery(String tableName) {
    return new SQLQuery("CREATE TABLE ${tableName} (col TEXT);")
        .withVariable("tableName", tableName);
  }

  protected void assertTmpTableSize(DataSourceProvider dsp, int itemCount) throws SQLException {
    assertTmpTableSize(dsp, getDefaultTmpTableName(), itemCount);
  }

  protected void assertTmpTableSize(DataSourceProvider dsp, String tableName, int itemCount) throws SQLException {
    assertEquals("The expected table count did not match the actual count.", itemCount,
        countTmpTableRows(dsp, tableName));
  }

  protected int countTmpTableRows(DataSourceProvider dsp) throws SQLException {
    return countTmpTableRows(dsp, getDefaultTmpTableName());
  }

  private static int countTmpTableRows(DataSourceProvider dsp, String tableName) throws SQLException {
    return (int) new SQLQuery("SELECT count(1) as count FROM ${tableName}")
        .withVariable("tableName", tableName)
        .run(dsp, rs -> rs.next() ? rs.getInt("count") : null);
  }

  protected enum QueryType {
    WRITE,
    READ,
    BATCH_WRITE
  }

  protected static CompletableFuture runQueryInThread(SQLQuery query, DataSourceProvider dsp, QueryType queryType) {
    CompletableFuture future = new CompletableFuture();
    new Thread(() -> {
      try {
        switch (queryType) {
          case WRITE -> query.write(dsp);
          case READ -> query.run(dsp, rs -> rs.next() ? rs.getString(1) : null);
          case BATCH_WRITE -> query.writeBatch(dsp);
        }
        future.complete(null);
      }
      catch (SQLException e) {
        if (e.getCause() != null) {
          e.getCause().printStackTrace();
          if (e.getCause() instanceof SQLException sqlException)
            System.out.println("Code: " + sqlException.getSQLState());
        }
        fail(e.getMessage());
      }
    }).start();
    return future;
  }
}
