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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.here.xyz.jobs.steps.impl.transport.ExpressWriterDataSources.SCHEMA;
import static com.here.xyz.jobs.steps.impl.transport.ExpressWriterDataSources.qualified;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueriesForTests;

/**
 * Verifies that the express import writer of {@code /jobs/transport.sql} can be executed against a space
 * table which was created by the FeatureWriter test framework
 * ({@code XyzSpaceTableHelper.buildCreateSpaceTableQueriesForTests}).
 *
 * <p>This is the foundation for reusing the FeatureWriter test scenarios: if the express writer can write
 * into those tables, the whole scenario and assertion machinery of
 * {@code com.here.xyz.test.featurewriter.TestSuite} becomes usable for it.</p>
 *
 * <p>In contrast to {@link ExpressImportSqlIT}, this test does not stub the helper functions the express
 * writer relies on. It runs against the actually installed {@code ext.sql} implementations of
 * {@code max_bigint}, {@code xyz_random_string}, {@code xyz_reduce_precision} and
 * {@code xyz_create_history_partition}.</p>
 */
public class ExpressWriterSmokeIT {

  private static final String TEST_FEATURE = """
      {"type":"Feature","id":"id1","geometry":{"type":"Point","coordinates":[8,50,0]},\
      "properties":{"firstName":"Alice","age":35}}""";

  /**
   * The history branch of the express writer additionally calls {@code xyz_create_history_partition} on the
   * target table, therefore both history modes are covered here.
   */
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void writesStagedFeatureIntoFeatureWriterStyleTable(boolean historyEnabled) throws Exception {
    String table = "express_smoke_" + (historyEnabled ? "history" : "nohistory");
    String stagingTable = table + "_staging";

    try (DataSourceProvider dsp = ExpressWriterDataSources.getDataSourceProvider()) {
      dropResources(dsp, table, stagingTable);
      createResources(dsp, table, stagingTable);

      new SQLQuery("INSERT INTO ${schema}.${table} (jsondata) VALUES (#{jsondata})")
          .withVariable("schema", SCHEMA)
          .withVariable("table", stagingTable)
          .withNamedParameter("jsondata", TEST_FEATURE)
          .write(dsp);

      long targetVersion = allocateVersion(dsp, table);

      BatchResult firstBatch = executeExpressImportBatch(dsp, stagingTable, table, 1, targetVersion, historyEnabled);
      assertEquals(1, firstBatch.pulledCount(), "The single staged row should have been pulled.");
      assertTrue(firstBatch.pulledBytes() > 0, "The pulled byte size should have been reported.");
      assertEquals(1, firstBatch.selectedRangeStart());
      assertEquals(1, firstBatch.selectedRangeEnd());
      assertFalse(firstBatch.finished(), "A batch which pulled rows must not report the task as finished.");

      WrittenRow writtenRow = readHeadRow(dsp, table);
      assertNotNull(writtenRow, "The express writer should have written a HEAD row.");
      assertEquals("id1", writtenRow.id());
      assertEquals(targetVersion, writtenRow.version(), "The row should have been written at the allocated version.");
      assertEquals("I", writtenRow.operation());
      assertEquals("owner", writtenRow.author());
      assertEquals("Alice", writtenRow.firstName());
      assertNotNull(writtenRow.geo(), "The geometry should have been moved into the geo column.");
      assertTrue(writtenRow.geo().contains("\"coordinates\""), "Unexpected geometry: " + writtenRow.geo());

      //Resuming behind the last staged row is what signals a finished task to the tasked import protocol
      BatchResult emptyBatch = executeExpressImportBatch(dsp, stagingTable, table,
          firstBatch.selectedRangeEnd() + 1, targetVersion, historyEnabled);
      assertEquals(0, emptyBatch.pulledCount());
      assertTrue(emptyBatch.finished(), "A batch without any remaining row must report the task as finished.");
    }
    finally {
      try (DataSourceProvider dsp = ExpressWriterDataSources.getDataSourceProvider()) {
        dropResources(dsp, table, stagingTable);
      }
    }
  }

  /**
   * Covers the id generation path: a staged feature without a usable id has to get one generated, and that
   * generated id has to be persisted back into the staging table so that a resume or a retry of the same range
   * produces the same id again.
   *
   * <p>Staged features which already carry an id must be left untouched, which is the common case and the reason
   * why the write back is applied to the affected rows only.</p>
   */
  @Test
  void generatesMissingIdsAndPersistsThemBackIntoTheStagingTable() throws Exception {
    String table = "express_smoke_generated_ids";
    String stagingTable = table + "_staging";
    String featureWithoutId = "{\"type\":\"Feature\",\"properties\":{\"name\":\"no-id\"}}";
    String featureWithId = "{\"type\":\"Feature\",\"id\":\"has-id\",\"properties\":{\"name\":\"with-id\"}}";

    try (DataSourceProvider dsp = ExpressWriterDataSources.getDataSourceProvider()) {
      dropResources(dsp, table, stagingTable);
      createResources(dsp, table, stagingTable);

      for (String jsondata : new String[] {featureWithoutId, featureWithId})
        new SQLQuery("INSERT INTO ${schema}.${table} (jsondata) VALUES (#{jsondata})")
            .withVariable("schema", SCHEMA)
            .withVariable("table", stagingTable)
            .withNamedParameter("jsondata", jsondata)
            .write(dsp);

      long targetVersion = allocateVersion(dsp, table);
      BatchResult batch = executeExpressImportBatch(dsp, stagingTable, table, 1, targetVersion, false);
      assertEquals(2, batch.pulledCount());

      String generatedId = new SQLQuery("SELECT id FROM ${schema}.${table} WHERE id <> #{knownId}")
          .withVariable("schema", SCHEMA)
          .withVariable("table", table)
          .withNamedParameter("knownId", "has-id")
          .run(dsp, rs -> rs.next() ? rs.getString(1) : null);
      assertNotNull(generatedId, "An id should have been generated for the feature without one.");
      assertFalse(generatedId.isBlank(), "The generated id must not be blank.");

      //The generated id has to be visible in the staging table, otherwise a retry would generate a different one
      assertEquals(generatedId, stagedId(dsp, stagingTable, 1),
          "The generated id should have been written back into the staging table.");
      //A feature which already carried an id must not have been rewritten
      assertEquals(featureWithId, stagedJsondata(dsp, stagingTable, 2),
          "A staged feature which already had an id must be left untouched.");
    }
    finally {
      try (DataSourceProvider dsp = ExpressWriterDataSources.getDataSourceProvider()) {
        dropResources(dsp, table, stagingTable);
      }
    }
  }

  private String stagedId(DataSourceProvider dsp, String stagingTable, long i) throws Exception {
    return new SQLQuery("SELECT jsondata::JSONB->>'id' AS id FROM ${schema}.${table} WHERE i = #{i}")
        .withVariable("schema", SCHEMA)
        .withVariable("table", stagingTable)
        .withNamedParameter("i", i)
        .run(dsp, rs -> rs.next() ? rs.getString("id") : null);
  }

  private String stagedJsondata(DataSourceProvider dsp, String stagingTable, long i) throws Exception {
    return new SQLQuery("SELECT jsondata FROM ${schema}.${table} WHERE i = #{i}")
        .withVariable("schema", SCHEMA)
        .withVariable("table", stagingTable)
        .withNamedParameter("i", i)
        .run(dsp, rs -> rs.next() ? rs.getString("jsondata") : null);
  }

  private void createResources(DataSourceProvider dsp, String table, String stagingTable) throws Exception {
    SQLQuery.batchOf(buildCreateSpaceTableQueriesForTests(SCHEMA, table)).writeBatch(dsp);
    //Same shape as the temporary import table created by ImportQueryBuilder.buildTemporaryDataTableForImportQuery
    new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (jsondata TEXT, i BIGSERIAL PRIMARY KEY)")
        .withVariable("schema", SCHEMA)
        .withVariable("table", stagingTable)
        .write(dsp);
  }

  private void dropResources(DataSourceProvider dsp, String table, String stagingTable) throws Exception {
    //The version and branch sequences are OWNED BY columns of the space table, so they are dropped with it
    new SQLQuery("DROP TABLE IF EXISTS ${schema}.${spaceTable} CASCADE; DROP TABLE IF EXISTS ${schema}.${stagingTable};")
        .withVariable("schema", SCHEMA)
        .withVariable("spaceTable", table)
        .withVariable("stagingTable", stagingTable)
        .write(dsp);
  }

  private long allocateVersion(DataSourceProvider dsp, String table) throws Exception {
    return new SQLQuery("SELECT nextval(#{sequence})")
        .withNamedParameter("sequence", SCHEMA + ".\"" + table + "_version_seq\"")
        .run(dsp, rs -> rs.next() ? rs.getLong(1) : -1L);
  }

  private BatchResult executeExpressImportBatch(DataSourceProvider dsp, String stagingTable, String table,
      long rangeStart, long targetVersion, boolean historyEnabled) throws Exception {
    return new SQLQuery("""
        SELECT * FROM execute_express_import_batch(
            to_regclass(#{sourceTable}), to_regclass(#{targetTable}), NULL::REGCLASS, #{spaceContext},
            #{rangeStart}, #{targetMb}::NUMERIC, #{author}, #{currentVersion}, #{historyEnabled})
        """)
        .withNamedParameter("sourceTable", qualified(stagingTable))
        .withNamedParameter("targetTable", qualified(table))
        .withNamedParameter("spaceContext", "DEFAULT")
        .withNamedParameter("rangeStart", rangeStart)
        .withNamedParameter("targetMb", 10d)
        .withNamedParameter("author", "owner")
        .withNamedParameter("currentVersion", targetVersion)
        .withNamedParameter("historyEnabled", historyEnabled)
        .run(dsp, rs -> rs.next()
            ? new BatchResult(rs.getInt("pulled_count"), rs.getLong("pulled_bytes"), rs.getLong("selected_range_start"),
                rs.getLong("selected_range_end"), rs.getBoolean("finished"))
            : null);
  }

  private WrittenRow readHeadRow(DataSourceProvider dsp, String table) throws Exception {
    return new SQLQuery("""
        SELECT id, version, operation, author, jsondata#>>'{properties,firstName}' AS first_name,
               ST_AsGeojson(geo) AS geo
          FROM ${schema}.${table}
         WHERE next_version = max_bigint()
        """)
        .withVariable("schema", SCHEMA)
        .withVariable("table", table)
        .run(dsp, rs -> rs.next()
            ? new WrittenRow(rs.getString("id"), rs.getLong("version"), rs.getString("operation"),
                rs.getString("author"), rs.getString("first_name"), rs.getString("geo"))
            : null);
  }

  private record BatchResult(int pulledCount, long pulledBytes, long selectedRangeStart, long selectedRangeEnd,
      boolean finished) {}

  private record WrittenRow(String id, long version, String operation, String author, String firstName, String geo) {}
}
