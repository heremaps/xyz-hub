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
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.core.type.TypeReference;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ContextAwareEvent.SpaceContext;
import com.here.xyz.test.sql.base.SQLITBase;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * The counts of write_features for imports (countWritten).
 * NOTE: Tagged with a group which runs in CI (see pom.xml).
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

        //The unchanged b counts as updated only with history (new version), the deletion of the missing x not at all
        assertWritten(1, historyEnabled ? 2 : 1, 1,
            writeFeatures(dsp, schema, table, historyEnabled, batchMode, feature("a", "a2"), feature("b", "b1"), deletion("c"),
                feature("d", "d1"), deletion("x")));

        //Both are deleted already (x by a deletion marker)
        assertWritten(0, 0, 0, writeFeatures(dsp, schema, table, historyEnabled, batchMode, deletion("c"), deletion("x")));
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  /**
   * The context DEFAULT shows the base features too: writing one updates it, deleting it (marker H) deletes it, writing over the
   * marker inserts it.
   */
  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void countsTheWritesOfACompositeSpace(boolean historyEnabled, boolean batchMode) throws Exception {
    String base = table("base", historyEnabled, batchMode);
    String extension = table("ext", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, base);
      createSpaceTable(dsp, schema, extension);
      try {
        List<String> tables = List.of(base, extension);
        write(dsp, schema, List.of(base), null, historyEnabled, batchMode, false, false, "REPLACE", false,
            feature("a", "a1"), feature("b", "b1"));

        assertWritten(1, 1, 1, counts(write(dsp, schema, tables, SpaceContext.DEFAULT, historyEnabled, batchMode, false, true,
            "REPLACE", false, feature("a", "a2"), deletion("b"), feature("c", "c1"))));
        assertWritten(0, 0, 0, counts(write(dsp, schema, tables, SpaceContext.DEFAULT, historyEnabled, batchMode, false, true,
            "REPLACE", false, deletion("b"))));
        assertWritten(1, 0, 0, counts(write(dsp, schema, tables, SpaceContext.DEFAULT, historyEnabled, batchMode, false, true,
            "REPLACE", false, feature("b", "b2"))));
      }
      finally {
        dropSpaceTable(dsp, schema, extension);
        dropSpaceTable(dsp, schema, base);
      }
    }
  }

  /**
   * The context EXTENSION sees the extension only.
   */
  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void countsTheWritesOfTheExtensionOnly(boolean historyEnabled, boolean batchMode) throws Exception {
    String base = table("extbase", historyEnabled, batchMode);
    String extension = table("extonly", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, base);
      createSpaceTable(dsp, schema, extension);
      try {
        List<String> tables = List.of(base, extension);
        write(dsp, schema, List.of(base), null, historyEnabled, batchMode, false, false, "REPLACE", false, feature("a", "a1"));

        assertWritten(2, 0, 0, counts(write(dsp, schema, tables, SpaceContext.EXTENSION, historyEnabled, batchMode, false, true,
            "REPLACE", false, feature("a", "a2"), feature("e", "e1"))));
        assertWritten(0, 1, 1, counts(write(dsp, schema, tables, SpaceContext.EXTENSION, historyEnabled, batchMode, false, true,
            "REPLACE", false, feature("e", "e2"), deletion("a"))));
        assertWritten(0, 0, 0, counts(write(dsp, schema, tables, SpaceContext.EXTENSION, historyEnabled, batchMode, false, true,
            "REPLACE", false, deletion("a"))));
      }
      finally {
        dropSpaceTable(dsp, schema, extension);
        dropSpaceTable(dsp, schema, base);
      }
    }
  }

  /**
   * onExists = RETAIN and partial writes (with onNotExists = RETAIN, a missing feature is not written).
   */
  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void countsTheWritesByTheUpdateStrategy(boolean historyEnabled, boolean batchMode) throws Exception {
    String table = table("strategy", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, table);
      try {
        List<String> tables = List.of(table);
        write(dsp, schema, tables, SpaceContext.DEFAULT, historyEnabled, batchMode, false, false, "REPLACE", false, feature("a", "a1"));

        assertWritten(1, 0, 0, counts(write(dsp, schema, tables, SpaceContext.DEFAULT, historyEnabled, batchMode, false, true,
            "RETAIN", false, feature("a", "a2"), feature("b", "b1"))));
        assertWritten(1, 1, 0, counts(write(dsp, schema, tables, SpaceContext.DEFAULT, historyEnabled, batchMode, false, true,
            "REPLACE", true, feature("a", "a3"), feature("p", "p1"))));
        assertWritten(0, 1, 0, counts(write(dsp, schema, tables, SpaceContext.DEFAULT, historyEnabled, batchMode, false, true,
            "REPLACE", "RETAIN", true, feature("missing", "m1"), feature("a", "a4"))));
        assertEquals(3L, headCount(dsp, schema, table), "missing must not be created");
        //The delete strategy of imports
        assertWritten(0, 0, 1, counts(write(dsp, schema, tables, SpaceContext.DEFAULT, historyEnabled, batchMode, false, true,
            "DELETE", "RETAIN", false, feature("a", "a5"), feature("missing", "m1"))));
        assertEquals(2L, headCount(dsp, schema, table));
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  /**
   * An ID written more than once in a call is counted once, by what the space showed before and after the call.
   */
  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void countsAnIdWrittenTwiceInACallOnce(boolean historyEnabled, boolean batchMode) throws Exception {
    String table = table("twice", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, table);
      try {
        writeFeatures(dsp, schema, table, historyEnabled, batchMode, feature("b", "b1"), feature("c", "c1"));

        assertWritten(1, 1, 1, writeFeatures(dsp, schema, table, historyEnabled, batchMode, feature("a", "a1"), feature("a", "a2"),
            deletion("b"), deletion("b"), feature("c", "c2"), feature("c", "c3"), feature("x", "x1"), deletion("x")));
        assertEquals(2L, headCount(dsp, schema, table), "a and c");
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  /**
   * Like the Hub: no context and the full result, which must stay unchanged.
   */
  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void keepsTheFullResultOfAHubWrite(boolean historyEnabled, boolean batchMode) throws Exception {
    String table = table("hub", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, table);
      try {
        write(dsp, schema, table, null, historyEnabled, batchMode, false, false, feature("a", "a1"), feature("b", "b1"));

        //Counting requested as well, the full result wins
        Map<String, Object> result = XyzSerializable.deserialize(write(dsp, schema, table, null, historyEnabled, batchMode, true, true,
            deletion("a"), deletion("missing")), new TypeReference<Map<String, Object>>() {});
        assertEquals(List.of("a"), result.get("deleted"));
        assertEquals(List.of(), result.get("inserted"));
        assertEquals(List.of(), result.get("updated"));
        assertEquals(Set.of("type", "features", "inserted", "updated", "deleted"), result.keySet(),
            "The full result must not get any counts");
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  /**
   * An empty list of modifications returns no result, as before.
   */
  @Test
  void returnsNoFullResultForNoModifications() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      String table = "feature_writer_no_modifications";
      createSpaceTable(dsp, schema, table);
      try {
        Map<String, Object> queryContext = new FeatureWriterQueryContextBuilder()
            .withSchema(schema)
            .withTables(List.of(table))
            .withHistoryEnabled(false)
            .build();
        String result = new SQLQuery("""
            WITH ctx AS MATERIALIZED (SELECT context(#{context}::JSONB))
            SELECT write_features('[]', 'Modifications', 'someAuthor', true) FROM ctx
            """)
            .withNamedParameter("context", XyzSerializable.serialize(queryContext))
            .run(dsp, rs -> rs.next() ? rs.getString(1) : "no row");
        assertNull(result);
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  /**
   * Without a context, the feature gets loaded for the counting only.
   */
  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void countsTheDeletionsWithoutContext(boolean historyEnabled, boolean batchMode) throws Exception {
    String table = table("nocontext", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, table);
      try {
        write(dsp, schema, table, null, historyEnabled, batchMode, false, false, feature("a", "a1"));
        assertWritten(0, 0, 1, counts(write(dsp, schema, table, null, historyEnabled, batchMode, false, true, deletion("a"),
            deletion("missing"))));
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  /**
   * Like a Hub deletion: the compact result without countWritten stays the plain count, and the deletion still works.
   */
  @ParameterizedTest(name = "history={0}, batchMode={1}")
  @CsvSource({"false,false", "false,true", "true,false", "true,true"})
  void doesNotCountTheWritesOfOtherCallers(boolean historyEnabled, boolean batchMode) throws Exception {
    String table = table("nocount", historyEnabled, batchMode);

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      String schema = dsp.getDatabaseSettings().getSchema();
      createSpaceTable(dsp, schema, table);
      try {
        write(dsp, schema, table, null, historyEnabled, batchMode, false, false, feature("a", "a1"), feature("b", "b1"));

        Map<String, Object> result = XyzSerializable.deserialize(write(dsp, schema, table, null, historyEnabled, batchMode, false,
            false, deletion("a")), new TypeReference<Map<String, Object>>() {});
        assertEquals(Set.of("count"), result.keySet(), "Nothing must be counted without the request: " + result);
        assertEquals(1L, headCount(dsp, schema, table), "a has to be deleted, b remains");
      }
      finally {
        dropSpaceTable(dsp, schema, table);
      }
    }
  }

  private static long headCount(DataSourceProvider dsp, String schema, String table) throws Exception {
    return new SQLQuery("SELECT count(*) FROM ${schema}.${table} WHERE next_version = max_bigint() AND operation NOT IN ('D', 'H', 'J')")
        .withVariable("schema", schema)
        .withVariable("table", table)
        .run(dsp, rs -> rs.next() ? rs.getLong(1) : -1L);
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
    return counts(write(dsp, schema, table, SpaceContext.DEFAULT, historyEnabled, batchMode, false, true, features));
  }

  private static Map<String, Integer> counts(String result) throws Exception {
    return XyzSerializable.deserialize(result, new TypeReference<Map<String, Integer>>() {});
  }

  private static String write(DataSourceProvider dsp, String schema, String table, SpaceContext context, boolean historyEnabled,
      boolean batchMode, boolean returnResult, boolean countWritten, String... features) throws Exception {
    return write(dsp, schema, List.of(table), context, historyEnabled, batchMode, returnResult, countWritten, "REPLACE", false, features);
  }

  /**
   * @param tables The base table first
   * @param context null for none
   */
  private static String write(DataSourceProvider dsp, String schema, List<String> tables, SpaceContext context, boolean historyEnabled,
      boolean batchMode, boolean returnResult, boolean countWritten, String onExists, boolean partial, String... features)
      throws Exception {
    return write(dsp, schema, tables, context, historyEnabled, batchMode, returnResult, countWritten, onExists, "CREATE", partial,
        features);
  }

  private static String write(DataSourceProvider dsp, String schema, List<String> tables, SpaceContext context, boolean historyEnabled,
      boolean batchMode, boolean returnResult, boolean countWritten, String onExists, String onNotExists, boolean partial,
      String... features) throws Exception {
    FeatureWriterQueryContextBuilder contextBuilder = new FeatureWriterQueryContextBuilder()
        .withSchema(schema)
        .withTables(tables)
        .withHistoryEnabled(historyEnabled)
        .withBatchMode(batchMode)
        .withCountWritten(countWritten);
    if (context != null)
      contextBuilder.withSpaceContext(context);

    //The context has to be set by the same statement (the pool might use another connection)
    return new SQLQuery("""
        WITH ctx AS MATERIALIZED (SELECT context(#{context}::JSONB))
        SELECT write_features(#{featureCollection}, 'FeatureCollection', 'someAuthor', #{returnResult}, NULL, #{onExists}, #{onNotExists},
            NULL, NULL, #{partial})
          FROM ctx
        """)
        .withNamedParameter("context", XyzSerializable.serialize(contextBuilder.build()))
        .withNamedParameter("featureCollection", "{\"type\":\"FeatureCollection\",\"features\":[" + String.join(",", features) + "]}")
        .withNamedParameter("returnResult", returnResult)
        .withNamedParameter("onExists", onExists)
        .withNamedParameter("onNotExists", onNotExists)
        .withNamedParameter("partial", partial)
        .run(dsp, rs -> rs.next() ? rs.getString(1) : null);
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
