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

package com.here.xyz.jobs.steps.impl.transport;

import java.util.List;

import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.events.ContextAwareEvent.SpaceContext.SUPER;
import com.here.xyz.events.UpdateStrategy;
import com.here.xyz.events.UpdateStrategy.OnExists;
import com.here.xyz.events.UpdateStrategy.OnMergeConflict;
import com.here.xyz.events.UpdateStrategy.OnNotExists;
import com.here.xyz.events.UpdateStrategy.OnVersionConflict;
import static com.here.xyz.jobs.steps.impl.transport.ExpressWriterDataSources.SCHEMA;
import static com.here.xyz.jobs.steps.impl.transport.ExpressWriterDataSources.qualified;
import com.here.xyz.jobs.steps.impl.transport.TaskedImportFilesToSpace.EntityPerLine;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.test.featurewriter.SpaceWriter;
import com.here.xyz.test.featurewriter.sql.SQLSpaceWriter;
import com.here.xyz.util.db.ConnectorParameters.TableLayout;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.SQLError;

/**
 * A {@link SpaceWriter} which writes features through the express import writer
 * ({@code execute_express_import_batch} of {@code /jobs/transport.sql}) instead of through the PLV8
 * FeatureWriter ({@code write_features}).
 *
 * <p>This makes the scenarios and assertions of {@code com.here.xyz.test.featurewriter.TestSuite} usable for
 * the express writer, so that both write implementations are verified against exactly the same expectations.</p>
 *
 * <h2>Writer selection</h2>
 * <p>The express writer only supports the default update strategy, so most of the FeatureWriter scenarios are
 * out of its scope. Instead of skipping those, this writer reproduces the production behavior and falls back to
 * the FeatureWriter for them, by asking the same capability check the production code uses
 * ({@code TaskedImportFilesToSpace.supportsExpressImport}). As a consequence the complete scenario tables keep
 * passing, and {@link #lastUsedWriterMode()} tells which implementation was actually exercised. Because a
 * fallback can hide that the express writer is never selected at all,
 * {@code ExpressRoutingGuardIT} pins the expected selection.</p>
 *
 * <h2>How the express writer is driven</h2>
 * <p>In production the express writer is the second phase of a tasked import: the first phase bulk-loads a file
 * into a temporary staging table {@code (jsondata TEXT, i BIGSERIAL)}, then
 * {@code perform_express_import_from_tmp_table_task} repeatedly writes bounded ranges out of it and reports
 * progress through a lambda callback. This writer reproduces only the write part: it stages the features and
 * calls {@code execute_express_import_batch} directly, looping over the returned ranges the same way
 * {@code TaskedImportFilesToSpace.onTaskProgress} does ({@code rangeStart = endI + 1}). The lambda callback
 * wrapper is deliberately skipped, as it carries no write semantics.</p>
 *
 * <h2>Version allocation</h2>
 * <p>The FeatureWriter allocates its own version from the target table's version sequence
 * ({@code FeatureWriter.getNextVersion}), whereas the express writer receives the version as a parameter,
 * because a tasked import writes all of its batches at one pre-allocated target version. This writer therefore
 * allocates from the very same sequence, which keeps the version bookkeeping of
 * {@code TestSuite.writtenSpaceVersions} valid for both implementations.</p>
 *
 * <h2>Known blind spot of the reused assertions</h2>
 * <p>{@code FeatureWriter.js} strips {@code properties.@ns:com:here:xyz.version} (among others) before writing
 * {@code jsondata}, while the express writer keeps the incoming namespace as-is apart from
 * {@code createdAt}/{@code updatedAt}. A stale input version can therefore survive inside the stored JSON.
 * {@code SpaceWriter.toFeature} overwrites {@code version} from the {@code version} column before the
 * comparison, so the assertions of {@code TestSuite} cannot observe that difference. The same applies to the
 * intentionally skipped stripping of {@code null} values. Detecting those would require comparing the raw
 * {@code jsondata} instead of the deserialized {@code Feature}, which is a change to the shared assertion
 * engine and thus out of scope here.</p>
 */
public class ExpressSpaceWriter extends SpaceWriter {

  /**
   * The byte budget of one express batch. Chosen large enough that all features of a test are written by a
   * single batch, while the range loop in {@link #writeThroughExpressWriter} still handles a split.
   */
  private static final double TARGET_MB = 10;

  /**
   * Used to create and drop the space tables, so that the express writer provably operates on exactly the same
   * table layout the FeatureWriter tests use, and to perform the writes the express writer does not support.
   */
  private final SQLSpaceWriter featureWriter;

  /**
   * The write implementation which was used for the last write. Kept per instance on purpose, because the test
   * suites may be executed concurrently.
   */
  private WriterMode lastUsedWriterMode;

  public ExpressSpaceWriter(boolean composite, String testSuiteName) {
    super(composite, testSuiteName);
    //NOTE: spaceId() is used instead of testSuiteName, because SpaceWriter resolves a null name to a class name
    featureWriter = new SQLSpaceWriter(composite, spaceId());
  }

  /**
   * @return The write implementation which performed the last write, or {@code null} if nothing was written yet.
   */
  public WriterMode lastUsedWriterMode() {
    return lastUsedWriterMode;
  }

  /**
   * The write implementations a tasked import can choose from.
   */
  public enum WriterMode {
    /** {@code execute_express_import_batch}, pure PostgreSQL. */
    EXPRESS,
    /** {@code write_features}, the PLV8 FeatureWriter. */
    FEATURE_WRITER
  }

  /**
   * @return The name of the table which takes the role of the temporary import table of a tasked import.
   */
  private String stagingTable() {
    return spaceId() + "_staging";
  }

  @Override
  public void createSpaceResources() throws Exception {
    featureWriter.createSpaceResources();
    try (DataSourceProvider dsp = ExpressWriterDataSources.getDataSourceProvider()) {
      //Same shape as ImportQueryBuilder.buildTemporaryDataTableForImportQuery creates it
      new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (jsondata TEXT, i BIGSERIAL PRIMARY KEY)")
          .withVariable("schema", SCHEMA)
          .withVariable("table", stagingTable())
          .write(dsp);
    }
  }

  @Override
  public void cleanSpaceResources() throws Exception {
    try (DataSourceProvider dsp = ExpressWriterDataSources.getDataSourceProvider()) {
      new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table};")
          .withVariable("schema", SCHEMA)
          .withVariable("table", stagingTable())
          .write(dsp);
    }
    featureWriter.cleanSpaceResources();
  }

  @Override
  protected void writeFeatures(List<Feature> featureList, String author, OnExists onExists, OnNotExists onNotExists,
      OnVersionConflict onVersionConflict, OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext,
      boolean historyEnabled, SQLError expectedErrorCode) throws Exception {
    lastUsedWriterMode = writerModeFor(onExists, onNotExists, onVersionConflict, onMergeConflict, isPartial, spaceContext);

    if (lastUsedWriterMode == WriterMode.FEATURE_WRITER) {
      /*
      NOTE: The public overload is used because a protected member of SpaceWriter can not be accessed through a
      reference of a different subtype. It forwards a null expectedErrorCode, which is exactly what TestSuite
      passes anyway, since TestSuite evaluates the thrown error itself.
       */
      featureWriter.writeFeatures(featureList, author, onExists, onNotExists, onVersionConflict, onMergeConflict,
          isPartial, spaceContext, historyEnabled);
      return;
    }

    writeThroughExpressWriter(featureList, author, spaceContext, historyEnabled);
  }

  /**
   * Decides which write implementation to use, by asking the very same capability check the production code uses
   * in {@code TaskedImportFilesToSpace.useExpressImport}. Everything the express writer does not support falls
   * back to the FeatureWriter, which is also what a real import does.
   *
   * <p>The update strategy is normalized before the check, because {@code TestSuite} expresses "use the
   * default" as {@code null} while {@code FeatureWriter.js} applies its defaults itself
   * ({@code onExists || "REPLACE"} and {@code onNotExists || "CREATE"}). Without normalizing, a scenario which
   * only sets {@code onNotExists = CREATE} would be misread as a non-default strategy.</p>
   *
   * <p>Kept static and package visible so that {@code ExpressRoutingGuardTest} can pin the selection without a
   * database.</p>
   */
  static WriterMode writerModeFor(OnExists onExists, OnNotExists onNotExists, OnVersionConflict onVersionConflict,
      OnMergeConflict onMergeConflict, boolean isPartial, SpaceContext spaceContext) {
    /*
    Partial writes are patched onto the current HEAD feature by the FeatureWriter. The express writer has no
    patch logic at all, and a tasked import never requests partial writes.
     */
    if (isPartial)
      return WriterMode.FEATURE_WRITER;

    UpdateStrategy updateStrategy = new UpdateStrategy(
        onExists == null ? OnExists.REPLACE : onExists,
        onNotExists == null ? OnNotExists.CREATE : onNotExists,
        onVersionConflict,
        onMergeConflict
    );

    return TaskedImportFilesToSpace.supportsExpressImport(true, EntityPerLine.Feature, updateStrategy,
        TableLayout.OLD_LAYOUT, spaceContext) ? WriterMode.EXPRESS : WriterMode.FEATURE_WRITER;
  }

  private void writeThroughExpressWriter(List<Feature> featureList, String author, SpaceContext spaceContext,
      boolean historyEnabled) throws Exception {
    try (DataSourceProvider dsp = ExpressWriterDataSources.getDataSourceProvider()) {
      stageFeatures(dsp, featureList);
      long targetVersion = allocateTargetVersion(dsp);

      /*
      All batches of one task are written at the same target version, exactly as a tasked import does.
      The range arithmetic mirrors TaskedImportFilesToSpace.onTaskProgress, which continues behind the
      last written row of the previous batch.
       */
      long rangeStart = 1;
      while (true) {
        BatchResult result = executeExpressImportBatch(dsp, rangeStart, targetVersion, author, spaceContext, historyEnabled);
        if (result == null)
          throw new IllegalStateException("The express writer did not return a batch result.");
        if (result.finished())
          return;
        rangeStart = result.selectedRangeEnd() + 1;
      }
    }
  }

  private void stageFeatures(DataSourceProvider dsp, List<Feature> featureList) throws Exception {
    //RESTART IDENTITY keeps the i values (and therefore the ranges) predictable across multiple writes
    new SQLQuery("TRUNCATE TABLE ${schema}.${table} RESTART IDENTITY;")
        .withVariable("schema", SCHEMA)
        .withVariable("table", stagingTable())
        .write(dsp);

    for (Feature feature : featureList)
      new SQLQuery("INSERT INTO ${schema}.${table} (jsondata) VALUES (#{jsondata})")
          .withVariable("schema", SCHEMA)
          .withVariable("table", stagingTable())
          .withNamedParameter("jsondata", feature.serialize())
          .write(dsp);
  }

  /**
   * Allocates the target version from the same sequence the FeatureWriter uses in
   * {@code FeatureWriter.getNextVersion}, so that both implementations produce the same version numbers for the
   * same sequence of writes.
   */
  private long allocateTargetVersion(DataSourceProvider dsp) throws Exception {
    return new SQLQuery("SELECT nextval(#{sequence})")
        .withNamedParameter("sequence", SCHEMA + ".\"" + spaceId() + "_version_seq\"")
        .run(dsp, rs -> rs.next() ? rs.getLong(1) : -1L);
  }

  private BatchResult executeExpressImportBatch(DataSourceProvider dsp, long rangeStart, long targetVersion, String author,
      SpaceContext spaceContext, boolean historyEnabled) throws Exception {
    return new SQLQuery("""
        SELECT * FROM execute_express_import_batch(
            to_regclass(#{sourceTable}), to_regclass(#{targetTable}), to_regclass(#{superTable}), #{spaceContext},
            #{rangeStart}, #{targetMb}::NUMERIC, #{author}, #{currentVersion}, #{historyEnabled})
        """)
        .withNamedParameter("sourceTable", qualified(stagingTable()))
        .withNamedParameter("targetTable", qualified(targetTable(spaceContext)))
        .withNamedParameter("superTable", visibleSuperTable(spaceContext) == null
            ? null : qualified(visibleSuperTable(spaceContext)))
        .withNamedParameter("spaceContext", spaceContext.name())
        .withNamedParameter("rangeStart", rangeStart)
        .withNamedParameter("targetMb", TARGET_MB)
        .withNamedParameter("author", author)
        .withNamedParameter("currentVersion", targetVersion)
        .withNamedParameter("historyEnabled", historyEnabled)
        .run(dsp, rs -> rs.next()
            ? new BatchResult(rs.getInt("pulled_count"), rs.getLong("selected_range_end"), rs.getBoolean("finished"))
            : null);
  }

  /**
   * The express writer never writes into a super space, it always writes into the space the import targets.
   */
  private String targetTable(SpaceContext spaceContext) {
    if (spaceContext == SUPER)
      throw new IllegalArgumentException("Importing data with context SUPER is not supported.");
    return spaceId();
  }

  /**
   * Mirrors {@code ImportQueryBuilder.buildExpressImportFromTmpTableTaskQuery}: only for context
   * {@code DEFAULT} the super space is visible, and therefore only then it may influence the operation of a
   * deletion. For context {@code EXTENSION} the import writes into the extension space alone.
   */
  private String visibleSuperTable(SpaceContext spaceContext) {
    return composite && spaceContext == DEFAULT ? superSpaceId() : null;
  }

  private record BatchResult(int pulledCount, long selectedRangeEnd, boolean finished) {}
}
