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

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.datasource.PooledDataSources;
import java.sql.SQLException;
import java.util.List;

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
}
