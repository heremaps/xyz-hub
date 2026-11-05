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

package com.here.xyz.psql.query;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueriesForTests;

import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.geojson.implementation.Properties;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.runtime.LambdaFunctionRuntime;
import com.here.xyz.util.service.aws.lambda.SimulatedContext;
import java.util.List;

public abstract class QueryTestBase {
  protected static final String PG_HOST = "localhost";
  protected static final String PG_DB = "postgres";
  protected static final String PG_USER = "postgres";
  protected static final String PG_PW = "password";
  protected static final DatabaseSettings DB_SETTINGS = new DatabaseSettings("testPSQL")
      .withApplicationName(QueryTestBase.class.getSimpleName())
      .withHost(PG_HOST)
      .withDb(PG_DB)
      .withUser(PG_USER)
      .withPassword(PG_PW)
      .withDbMaxPoolSize(2)
      .withScriptResourcePaths(List.of(new ScriptResourcePath("/sql", "hub", "common")));
  protected static final String PG_SCHEMA = "public";
  public static final String TEST_AUTHOR = "Test Author";

  static {
    new LambdaFunctionRuntime(new SimulatedContext("localLambda", null),
        QueryTestBase.class.getSimpleName() + "_" + Random.randomAlpha(4));
  }

  protected static DataSourceProvider getDataSourceProvider() {
    return getDataSourceProvider(DB_SETTINGS);
  }

  protected static DataSourceProvider getDataSourceProvider(DatabaseSettings dbSettings) {
    return new PooledDataSources(dbSettings);
  }

  protected static void createSpaceTable(String schema, String table) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery.batchOf(buildCreateSpaceTableQueriesForTests(schema, table)).writeBatch(dsp);
    }
  }

  protected static void dropTable(String schema, String table) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table} CASCADE")
          .withVariable("schema", schema)
          .withVariable("table", table)
          .write(dsp);
    }
  }

  protected static Feature newTestFeature(String id) {
    return newTestFeature(id, false);
  }

  @Deprecated
  protected static Feature newTestFeature(String id, boolean addRandomProperties) {
    return new Feature()
        .withId(id)
        .withProperties(addRandomProperties ? new Properties().with("name", id + Random.randomAlpha(5)) : null)
        .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 0)));
  }

  protected final String spaceId() {
    return getClass().getSimpleName();
  }
}
