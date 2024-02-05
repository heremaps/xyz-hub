/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.psql.query.branching;

import static com.here.xyz.events.UpdateStrategy.DEFAULT_UPDATE_STRATEGY;
import static com.here.xyz.psql.query.branching.BranchManager.getNodeId;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueries;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.amazonaws.services.lambda.runtime.Context;
import com.here.xyz.events.GetFeaturesByIdEvent;
import com.here.xyz.events.WriteFeaturesEvent.Modification;
import com.here.xyz.models.geojson.coordinates.PointCoordinates;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.Point;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.psql.query.GetFeaturesById;
import com.here.xyz.util.Random;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.datasource.DatabaseSettings;
import com.here.xyz.util.db.datasource.DatabaseSettings.ScriptResourcePath;
import com.here.xyz.util.db.datasource.PooledDataSources;
import com.here.xyz.util.runtime.LambdaFunctionRuntime;
import com.here.xyz.util.service.aws.lambda.SimulatedContext;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

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
    Context ctx = new SimulatedContext("localLambda", null);
    new LambdaFunctionRuntime(ctx, baseStreamId());
  }

  protected static DataSourceProvider getDataSourceProvider() {
    return new PooledDataSources(DB_SETTINGS);
  }

  protected static DataSourceProvider getDataSourceProvider(DatabaseSettings dbSettings) {
    return new PooledDataSources(dbSettings);
  }

  protected static void createSpaceTable(String schema, String table) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      SQLQuery.batchOf(buildCreateSpaceTableQueries(schema, table)).writeBatch(dsp);
    }
  }

  protected void dropSpaceTables(String schema, String rootTableName) throws Exception {
    dropTable(schema, rootTableName);
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      branchManager(dsp).deleteBranch(1);
      branchManager(dsp).deleteBranch(2);
      //TODO: Get all branch table names and drop also these
    }
    catch (SQLException e) {
      //E.g. not found
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

  protected static Ref branchRefOf(int nodeId) {
    return Ref.fromBranchId("~" + nodeId);
  }

  private String defaultTestRootTable() {
    return this.getClass().getSimpleName();
  }

  private String streamId() {
    return this.getClass().getSimpleName() + "_" + Random.randomAlpha(4);
  }

  private static String baseStreamId() {
    return QueryTestBase.class.getSimpleName() + "_" + Random.randomAlpha(4);
  }

  protected BranchManager branchManager(DataSourceProvider dsp) {
    return new BranchManager(dsp, streamId(), defaultTestRootTable(), PG_SCHEMA, defaultTestRootTable());
  }

  private static BranchManager branchManager(DataSourceProvider dsp, String rootTableName) {
    return new BranchManager(dsp, baseStreamId(), rootTableName, PG_SCHEMA, rootTableName);
  }

  protected Feature readFeatureById(String id, Ref versionRef) throws Exception {
    return readFeatureById(id, versionRef, PG_SCHEMA, defaultTestRootTable());
  }

  protected static Feature readFeatureById(String id, Ref versionRef, String schema, String rootTableName) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      branchManager(dsp, rootTableName);
      int nodeId = getNodeId(versionRef);
      GetFeaturesByIdEvent event = new GetFeaturesByIdEvent()
          .withSpace(rootTableName)
          .withIds(List.of(id))
          .withVersionsToKeep(1000)
          .withNodeId(nodeId)
          .withBranchPath(branchManager(dsp, rootTableName).branchPath(nodeId));
      FeatureCollection fc = new GetFeaturesById(event)
          .withDataSourceProvider(dsp)
          .run();
      assertTrue(fc.getFeatures().size() <= 1);
      return fc.getFeatures().size() == 0 ? null : fc.getFeatures().get(0);
    }
  }

  protected void writeFeature(Feature feature, Ref versionRef) throws Exception {
    writeFeature(feature, versionRef, defaultTestRootTable());
  }

  protected static void writeFeature(Feature feature, Ref versionRef, String rootTableName) throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      BranchManager bm = branchManager(dsp, rootTableName);
      bm.writeCommit(
          getNodeId(versionRef),
          Set.of(new Modification()
              .withFeatureData(new FeatureCollection().withFeatures(List.of(feature)))
              .withUpdateStrategy(DEFAULT_UPDATE_STRATEGY)),
          TEST_AUTHOR,
          versionRef
      );
    }
  }

  protected static Feature newTestFeature(String id) {
    return new Feature()
        .withId(id)
        .withGeometry(new Point().withCoordinates(new PointCoordinates(0, 0)));
  }
}
