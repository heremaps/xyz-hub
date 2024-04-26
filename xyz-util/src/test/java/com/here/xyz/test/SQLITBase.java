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

import com.here.xyz.util.db.DatabaseSettings;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.PooledDataSources;

public class SQLITBase {

  protected static final String PG_HOST = "localhost";
  protected static final String PG_DB = "postgres";
  protected static final String PG_USER = "postgres";
  protected static final String PG_PW = "password";
  protected static final DatabaseSettings DB_SETTINGS = new DatabaseSettings("testPSQL")
      .withHost(PG_HOST)
      .withDb(PG_DB)
      .withUser(PG_USER)
      .withPassword(PG_PW)
      .withDbMaxPoolSize(2);

  protected static DataSourceProvider getDataSourceProvider() {
    return new PooledDataSources(DB_SETTINGS);
  }

  protected static DataSourceProvider getDataSourceProvider(DatabaseSettings dbSettings) {
    return new PooledDataSources(dbSettings);
  }
}
