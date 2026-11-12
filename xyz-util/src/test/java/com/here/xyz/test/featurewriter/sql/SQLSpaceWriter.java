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

package com.here.xyz.test.featurewriter.sql;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueriesForTests;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import com.here.xyz.events.WriteFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.test.sql.base.SQLITBase;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import com.here.xyz.util.db.pg.LazySpaceTables;
import com.here.xyz.util.db.pg.LazySpaceTables.Level;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelCheck;
import com.here.xyz.util.db.pg.LazySpaceTables.LevelState;
import com.here.xyz.util.db.pg.LazySpaceTables.Profile;
import com.here.xyz.util.db.pg.LazySpaceTables.TemplateProbe;
import com.here.xyz.util.db.pg.SQLError;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SQLSpaceWriter extends SpaceWriter {
  private static final Logger logger = LogManager.getLogger();
  protected static String VERSION_SEQUENCE_SUFFIX = "_version_seq";
  private static final long MATERIALIZATION_BUDGET_MILLIS = 20_000;
  private boolean batchMode;
  private boolean lazyTables;

  public SQLSpaceWriter(boolean composite, String testSuiteName) {
    super(composite, testSuiteName);
  }

  public SQLSpaceWriter withBatchMode(boolean batchMode) {
    this.batchMode = batchMode;
    return this;
  }

  /**
   * In the lazy mode, the space tables are not created upfront. A write materializes its target table, and a table that does not
   * exist is read from the empty-space template, as for a lazy space.
   */
  public SQLSpaceWriter withLazyTables(boolean lazyTables) {
    this.lazyTables = lazyTables;
    return this;
  }

  @Override
  protected String doResolveTableName(String spaceId) {
    if (!lazyTables)
      return super.doResolveTableName(spaceId);
    try (DataSourceProvider dsp = SQLITBase.getDataSourceProvider()) {
      return tableOrTemplate(dsp, spaceId);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * @return Whether the table of the space or of its super space exists
   */
  public boolean tableExists(SpaceContext context) throws SQLException {
    String table = context == SpaceContext.SUPER ? superSpaceId() : spaceId();
    try (DataSourceProvider dsp = SQLITBase.getDataSourceProvider()) {
      return !tableOrTemplate(dsp, table).equals(LazySpaceTables.TEMPLATE_TABLE);
    }
    catch (SQLException e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String tableOrTemplate(DataSourceProvider dsp, String table) throws SQLException {
    LevelCheck check = LazySpaceTables.checkLevels(dsp, dsp.getDatabaseSettings().getSchema(), List.of(new Level(0, table, table)));
    return check.states().get(0) == LevelState.EXISTS ? table : LazySpaceTables.TEMPLATE_TABLE;
  }

  @Override
  public void createSpaceResources() throws Exception {
    if (lazyTables) {
      ensureTemplate();
      return;
    }
    try (DataSourceProvider dsp = SQLITBase.getDataSourceProvider()) {
      List<SQLQuery> queries = new ArrayList<>();
      if (composite)
        queries.addAll(buildCreateSpaceTableQueriesForTests(dsp.getDatabaseSettings().getSchema(), superSpaceId()));
      queries.addAll(buildCreateSpaceTableQueriesForTests(dsp.getDatabaseSettings().getSchema(), spaceId()));
      SQLQuery.batchOf(queries).writeBatch(dsp);
    }
  }

  /**
   * Installs the empty-space template into the schema of the tests if it is absent, as the script check of a connector does.
   */
  private static void ensureTemplate() throws Exception {
    try (DataSourceProvider dsp = SQLITBase.getDataSourceProvider(); Connection connection = dsp.getWriter().getConnection()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      TemplateProbe concurrentProbe = LazySpaceTables.installTemplate(connection, schema, dsp.getDatabaseSettings().getSearchPath(),
          MATERIALIZATION_BUDGET_MILLIS);
      if (concurrentProbe != null && !concurrentProbe.isIdentical())
        throw new IllegalStateException("The empty-space template in schema " + schema + " is not identical: "
            + concurrentProbe.differences());
    }
  }

  @Override
  public void cleanSpaceResources() throws Exception {
    try (DataSourceProvider dsp = SQLITBase.getDataSourceProvider()) {
      buildDropSpaceQuery(dsp.getDatabaseSettings().getSchema(), spaceId()).write(dsp);

      if (composite)
        buildDropSpaceQuery(dsp.getDatabaseSettings().getSchema(), superSpaceId()).write(dsp);
    }
  }

  private SQLQuery buildDropSpaceQuery(String schema, String table) {
    return new SQLQuery("${{dropTable}} ${{dropISequence}} ${{dropVersionSequence}}").withQueryFragment("dropTable",
            "DROP TABLE IF EXISTS ${schema}.${table};").withQueryFragment("dropISequence", "DROP SEQUENCE IF EXISTS ${schema}.${iSequence};")
        .withQueryFragment("dropVersionSequence", "DROP SEQUENCE IF EXISTS ${schema}.${versionSequence};").withVariable(SCHEMA, schema)
        .withVariable(TABLE, table).withVariable("iSequence", table + VERSION_SEQUENCE_SUFFIX)
        .withVariable("versionSequence", table + VERSION_SEQUENCE_SUFFIX);
  }

  @Override
  protected void writeFeatures(List<Feature> featureList, String author, OnExists onExists,
      OnNotExists onNotExists, OnVersionConflict onVersionConflict,
      OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext, boolean historyEnabled,
      SQLError expectedErrorCode) throws Exception {
    runWriteFeatureQuery(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, spaceContext,
        historyEnabled);
  }

  private int[] runWriteFeatureQuery(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean historyEnabled) throws Exception {
    try (DataSourceProvider dsp = SQLITBase.getDataSourceProvider()) {
      long tStart = System.currentTimeMillis();
      List<String> tables = composite ? List.of(superSpaceId(), spaceId()) : List.of(spaceId());
      if (lazyTables)
        tables = materializeTarget(dsp, tables, composite && spaceContext == SpaceContext.SUPER ? superSpaceId() : spaceId());
      SQLQuery q = generateWriteFeatureQuery(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict,
          isPartial, spaceContext, historyEnabled, tables);

      int[] result = SQLQuery.batchOf(q).writeBatch(dsp);
      System.out.println("SQLQuery " + q.getQueryId() + " took: " + (System.currentTimeMillis() - tStart) + "ms.");
      return result;
    }
  }

  /**
   * Materializes the write target and replaces each other table that does not exist by the empty-space template.
   *
   * @return The tables for the feature writer
   */
  private static List<String> materializeTarget(DataSourceProvider dsp, List<String> tables, String target) throws SQLException {
    LazySpaceTables.materialize(dsp, dsp.getDatabaseSettings().getSchema(), target, target, Profile.FULL, List.of(), null,
        MATERIALIZATION_BUDGET_MILLIS);
    List<String> readableTables = new ArrayList<>();
    for (String table : tables)
      readableTables.add(table.equals(target) ? table : tableOrTemplate(dsp, table));
    return readableTables;
  }

  private SQLQuery generateWriteFeatureQuery(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean historyEnabled, List<String> tables) {
    final Map<String, Object> queryContext = new FeatureWriterQueryContextBuilder()
        .withSchema(getDataSourceProvider().getDatabaseSettings().getSchema())
        .withTables(tables)
        .withSpaceContext(spaceContext)
        .withHistoryEnabled(historyEnabled)
        .withBatchMode(batchMode)
        .build();

    WriteFeaturesEvent.Modification modification = new
            WriteFeaturesEvent.Modification()
            .withFeatureData(new FeatureCollection().withFeatures(featureList))
            .withUpdateStrategy(new UpdateStrategy(onExists, onNotExists, onVersionConflict, onMergeConflict))
            .withPartialUpdates(isPartial);

    return new SQLQuery("SELECT write_features(#{featureModificationList}, 'Modifications', #{author}, #{responseDataExpected});")
        .withNamedParameter("featureModificationList", XyzSerializable.serialize(Set.of(modification)))
        .withNamedParameter("author", author)
        .withNamedParameter("responseDataExpected", true)
        .withContext(queryContext);
  }
}
