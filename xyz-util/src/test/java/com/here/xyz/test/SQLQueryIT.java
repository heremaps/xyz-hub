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
import static org.junit.Assert.assertNull;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.StaticDataSources;
import java.sql.SQLException;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

public class SQLQueryIT {
  protected static final String PG_HOST = "localhost";
  protected static final String PG_DB = "postgres";
  protected static final String PG_USER = "postgres";
  protected static final String PG_PW = "password";

  @Test
  public void startAndKillQuery() throws SQLException {
    PGSimpleDataSource ds = new PGSimpleDataSource() ;
    ds.setServerName(PG_HOST);
    ds.setDatabaseName(PG_DB);
    ds.setUser(PG_USER);
    ds.setPassword(PG_PW);
    DataSourceProvider dsp = new StaticDataSources(ds);

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
    assertEquals(longRunningQuery.text(), new SQLQuery("SELECT query FROM pg_stat_activity "
        + "WHERE state = 'active' "
        + "AND strpos(query, #{labelValue}) > 0 "
        + "AND pid != pg_backend_pid()")
        .withNamedParameter("labelValue", longRunningQuery.getQueryId())
        .run(dsp, rs -> rs.next() ? rs.getString("query") : null));

    //Kill the long-running query
    longRunningQuery.kill();

    //Check that the original query is not running anymore
    assertNull(new SQLQuery("SELECT query FROM pg_stat_activity "
        + "WHERE state = 'active' "
        + "AND strpos(query, #{labelValue}) > 0 "
        + "AND pid != pg_backend_pid()")
        .withNamedParameter("labelValue", longRunningQuery.getQueryId())
        .run(dsp, rs -> rs.next() ? rs.getString("query") : null));
  }
}
