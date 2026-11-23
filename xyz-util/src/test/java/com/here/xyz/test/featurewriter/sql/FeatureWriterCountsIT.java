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

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueriesForTests;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.sql.base.SQLITBase;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Without return_result, write_features returns how many features it has inserted, updated and deleted, next to the count of
 * resulting features. The tasked import uses these counts for its statistics.
 * NOTE: Tagged with the group of the ITs which run in CI (see the failsafe configuration in pom.xml).
 */
@Tag("lazy-space-tables")
public class FeatureWriterCountsIT extends SQLITBase {

  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void returnsTheWrittenCounts(boolean historyEnabled, boolean batchMode) throws Exception {
    String table = table("counts", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, table);
      try {
        assertWritten(3, 0, 0,
            writeFeatures(dsp, schema, table, historyEnabled, batchMode, feature("a", "a1"), feature("b", "b1"), feature("c", "c1")));

        /*
        Without history the unchanged b is not written, with history it gets a new version. x did not exist, so its deletion does not
        count, although a deletion marker gets written for it.
        NOTE: "count" is the writer's own number of resulting features, which contains such a marker without history
         */
        assertWritten(1, historyEnabled ? 2 : 1, 1,
            writeFeatures(dsp, schema, table, historyEnabled, batchMode, feature("a", "a2"), feature("b", "b1"), deletion("c"),
                feature("d", "d1"), deletion("x")));

        //c is deleted already
        assertWritten(0, 0, 0, writeFeatures(dsp, schema, table, historyEnabled, batchMode, deletion("c")));
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  /**
   * Deleting a feature which never existed writes a deletion marker. Deleting it again removes (without history) or replaces (with
   * history) that marker. Neither changes what the space shows, so neither counts as a deletion.
   */
  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void doesNotCountTheRepeatedDeletionOfAMissingFeature(boolean historyEnabled, boolean batchMode) throws Exception {
    String table = table("missing", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, table);
      try {
        assertWritten(0, 0, 0, writeFeatures(dsp, schema, table, historyEnabled, batchMode, deletion("never")));
        assertWritten(0, 0, 0, writeFeatures(dsp, schema, table, historyEnabled, batchMode, deletion("never")));
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  /**
   * The mark of a deletion which did not change the space is kept by the writer, so a member of the same name in an input feature
   * can not change the counts.
   */
  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void ignoresAMarkerNameInTheInput(boolean historyEnabled, boolean batchMode) throws Exception {
    String table = table("input", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, table);
      try {
        String marked = "{\"type\":\"Feature\",\"id\":\"m\",\"invisibleBefore\":true,\"geometry\":{\"type\":\"Point\","
            + "\"coordinates\":[8,50,0]},\"properties\":{\"name\":\"m\"}}";
        assertWritten(1, 0, 0, writeFeatures(dsp, schema, table, historyEnabled, batchMode, marked));
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  private static String table(String name, boolean historyEnabled, boolean batchMode) {
    return "feature_writer_" + name + "_" + (historyEnabled ? "history_" : "nohistory_") + (batchMode ? "batch" : "single");
  }

  private static void assertWritten(int inserted, int updated, int deleted, Map<String, Integer> result) {
    assertEquals(Map.of("insertedCount", inserted, "updatedCount", updated, "deletedCount", deleted),
        Map.of("insertedCount", result.get("insertedCount"), "updatedCount", result.get("updatedCount"), "deletedCount",
            result.get("deletedCount")), "Unexpected counts in " + result);
  }

  private static Map<String, Integer> writeFeatures(DataSourceProvider dsp, String schema, String table, boolean historyEnabled,
      boolean batchMode, String... features) throws Exception {
    Map<String, Object> queryContext = new FeatureWriterQueryContextBuilder()
        .withSchema(schema)
        .withTables(List.of(table))
        .withSpaceContext(SpaceContext.DEFAULT)
        .withHistoryEnabled(historyEnabled)
        .withBatchMode(batchMode)
        .build();

    //The context has to be set by the same statement, as the pool might use another connection for a further one
    String result = new SQLQuery("""
        WITH ctx AS MATERIALIZED (SELECT context(#{context}::JSONB))
        SELECT write_features(#{featureCollection}, 'FeatureCollection', 'someAuthor', false, NULL, 'REPLACE', 'CREATE')
          FROM ctx
        """)
        .withNamedParameter("context", XyzSerializable.serialize(queryContext))
        .withNamedParameter("featureCollection", "{\"type\":\"FeatureCollection\",\"features\":[" + String.join(",", features) + "]}")
        .run(dsp, rs -> rs.next() ? rs.getString(1) : null);
    return XyzSerializable.deserialize(result, new TypeReference<Map<String, Integer>>() {});
  }

  private static void createSpaceTable(DataSourceProvider dsp, String schema, String table) throws Exception {
    dropSpaceTable(dsp, schema, table);
    SQLQuery.batchOf(buildCreateSpaceTableQueriesForTests(schema, table)).writeBatch(dsp);
  }

  private static void dropSpaceTable(DataSourceProvider dsp, String schema, String table) throws Exception {
    new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table} CASCADE")
        .withVariable("schema", schema)
        .withVariable("table", table)
        .write(dsp);
  }

  private static String feature(String id, String name) {
    return "{\"type\":\"Feature\",\"id\":\"" + id + "\",\"geometry\":{\"type\":\"Point\",\"coordinates\":[8,50,0]},"
        + "\"properties\":{\"name\":\"" + name + "\"}}";
  }

  private static String deletion(String id) {
    return "{\"type\":\"Feature\",\"id\":\"" + id + "\",\"properties\":{\"@ns:com:here:xyz\":{\"deleted\":true}}}";
  }
}
