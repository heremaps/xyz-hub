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

import static com.here.xyz.events.ContextAwareEvent.SpaceContext.DEFAULT;
import static com.here.xyz.util.Random.randomAlpha;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.buildCreateSpaceTableQueriesForTests;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.test.sql.base.SQLITBase;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.here.xyz.util.db.pg.FeatureWriterQueryBuilder.FeatureWriterQueryContextBuilder;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Deletions without conflict detection (onExists = DELETE, as used by the imports of deleted features) into spaces with history.
 * Such a deletion does not need the content of the existing feature, only whether it exists and in which datasets.
 */
public class SQLFeatureWriterDeletionIT extends SQLITBase {
  private static final String XYZ_NS = "@ns:com:here:xyz";
  private final String superTable = getClass().getSimpleName().toLowerCase() + "_" + randomAlpha(5).toLowerCase() + "_super";
  private final String table = superTable.replace("_super", "_ext");
  private DataSourceProvider dsp;
  private String schema;

  @BeforeEach
  public void createTables() throws Exception {
    dsp = getDataSourceProvider();
    schema = dsp.getDatabaseSettings().getSchema();
    List<SQLQuery> queries = new ArrayList<>(buildCreateSpaceTableQueriesForTests(schema, superTable));
    queries.addAll(buildCreateSpaceTableQueriesForTests(schema, table));
    SQLQuery.batchOf(queries).writeBatch(dsp);
  }

  @AfterEach
  public void dropTables() throws Exception {
    try {
      for (String t : List.of(table, superTable))
        new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table} CASCADE; DROP SEQUENCE IF EXISTS ${schema}.${sequence};")
            .withVariable("schema", schema).withVariable("table", t).withVariable("sequence", t + "_version_seq")
            .write(dsp);
    }
    finally {
      dsp.close();
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void existingFeatureIsDeletedMissingFeatureIsRetained(boolean batchMode) throws Exception {
    insertRow(table, "f1", 1, "I", 1000);
    insertRow(table, "f2", 1, "I", 2000);

    JsonObject result = delete(List.of(table), List.of("f1", "missing"), 2, "RETAIN", batchMode);

    assertEquals(List.of("f1"), result.getJsonArray("deleted").getList());
    assertEquals(List.of(
        new Row("f1", 1, false, "I", 1000),
        new Row("f1", 2, true, "D", 1000),
        new Row("f2", 1, true, "I", 2000)), rows(table));
    JsonObject deletedRow = deletedRowContent(table, "f1");
    assertEquals(Set.of("type", "id", "properties"), deletedRow.fieldNames());
    assertEquals(Set.of(XYZ_NS), deletedRow.getJsonObject("properties").fieldNames());
    JsonObject xyzNs = deletedRow.getJsonObject("properties").getJsonObject(XYZ_NS);
    assertEquals(Set.of("deleted", "createdAt", "updatedAt"), xyzNs.fieldNames());
    assertTrue(xyzNs.getBoolean("deleted"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void missingFeatureFailsForOnNotExistsError(boolean batchMode) throws Exception {
    insertRow(table, "f1", 1, "I", 1000);

    SQLException e = assertThrows(SQLException.class, () -> delete(List.of(table), List.of("f1", "missing"), 2, "ERROR", batchMode));

    assertEquals("XYZ44", e.getSQLState());
    assertEquals(List.of(new Row("f1", 1, true, "I", 1000)), rows(table));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void featureDeletedTwiceInOneWriteIsDeletedOnce(boolean batchMode) throws Exception {
    insertRow(table, "f1", 1, "I", 1000);

    JsonObject result = delete(List.of(table), List.of("f1", "f1"), 2, "RETAIN", batchMode);

    assertEquals(List.of("f1"), result.getJsonArray("deleted").getList());
    assertEquals(List.of(
        new Row("f1", 1, false, "I", 1000),
        new Row("f1", 2, true, "D", 1000)), rows(table));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void headWithoutCreationTimestampGetsMinusOne(boolean batchMode) throws Exception {
    insertRow(table, "f1", 1, "I", null);

    delete(List.of(table), List.of("f1"), 2, "RETAIN", batchMode);

    assertEquals(List.of(
        new Row("f1", 1, false, "I", 0),
        new Row("f1", 2, true, "D", -1)), rows(table));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void headWithTheWrittenVersionIsDeletedWithTheNextVersion(boolean batchMode) throws Exception {
    insertRow(table, "f1", 1, "I", 1000);
    new SQLQuery("SELECT setval('${schema}.${sequence}', 1)")
        .withVariable("schema", schema).withVariable("sequence", table + "_version_seq").run(dsp);

    //The HEAD has the version that is written, like a conflict with a concurrent write
    delete(List.of(table), List.of("f1"), 1, "RETAIN", batchMode);

    assertEquals(List.of(
        new Row("f1", 1, false, "I", 1000),
        new Row("f1", 2, true, "D", 1000)), rows(table));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void compositeDeletionWritesTheOperationOfTheContainingDatasets(boolean batchMode) throws Exception {
    insertRow(superTable, "inSuper", 1, "I", 100);
    insertRow(superTable, "inBoth", 1, "I", 200);
    insertRow(table, "inBoth", 1, "U", 200);
    insertRow(table, "inExtension", 1, "I", 300);
    long beforeDeletion = System.currentTimeMillis();

    JsonObject result = delete(List.of(superTable, table), List.of("inSuper", "inBoth", "inExtension", "missing"), 2, "RETAIN",
        batchMode);

    assertEquals(List.of("inSuper", "inBoth", "inExtension"), result.getJsonArray("deleted").getList());
    List<Row> rows = rows(table);
    Row hiddenSuperFeature = rows.stream().filter(row -> row.id().equals("inSuper")).findFirst().get();
    assertEquals(new Row("inSuper", 2, true, "H", hiddenSuperFeature.createdAt()), hiddenSuperFeature);
    assertTrue(hiddenSuperFeature.createdAt() >= beforeDeletion, "A hiding row is a creation in the extension");
    assertEquals(List.of(
        new Row("inBoth", 1, false, "U", 200),
        new Row("inBoth", 2, true, "J", 200),
        new Row("inExtension", 1, false, "I", 300),
        new Row("inExtension", 2, true, "D", 300)),
        rows.stream().filter(row -> !row.id().equals("inSuper")).toList());
    assertEquals(List.of(new Row("inBoth", 1, true, "I", 200), new Row("inSuper", 1, true, "I", 100)), rows(superTable));
  }

  private void insertRow(String targetTable, String id, long version, String operation, long createdAt) throws SQLException {
    insertRow(targetTable, id, version, operation, Long.valueOf(createdAt));
  }

  /**
   * @param createdAt The creation timestamp of the row, null for none
   */
  private void insertRow(String targetTable, String id, long version, String operation, Long createdAt) throws SQLException {
    new SQLQuery("""
        INSERT INTO ${schema}.${table} (id, version, operation, author, jsondata, geo)
        VALUES (#{id}, #{version}, #{operation}, 'writer',
                jsonb_build_object('type', 'Feature', 'id', #{id}::TEXT, 'properties', jsonb_build_object(
                    '@ns:com:here:xyz', jsonb_build_object('createdAt', #{createdAt}::BIGINT, 'updatedAt', #{createdAt}::BIGINT),
                    'name', #{id}::TEXT)),
                ST_Force3D(ST_SetSRID(ST_MakePoint(8.5, 50.1), 4326)))
        """)
        .withVariable("schema", schema).withVariable("table", targetTable)
        .withNamedParameter("id", id).withNamedParameter("version", version).withNamedParameter("operation", operation)
        .withNamedParameter("createdAt", createdAt)
        .write(dsp);
  }

  private JsonObject delete(List<String> tables, List<String> ids, long version, String onNotExists, boolean batchMode)
      throws SQLException {
    Map<String, Object> context = new FeatureWriterQueryContextBuilder()
        .withSchema(schema)
        .withTables(tables)
        .withSpaceContext(DEFAULT)
        .withHistoryEnabled(true)
        .withBatchMode(batchMode)
        .build();
    //The input features carry content, like the features of an import of deleted features
    JsonArray features = new JsonArray(ids.stream()
        .map(id -> new JsonObject()
            .put("type", "Feature")
            .put("id", id)
            .put("properties", new JsonObject().put("name", id))
            .put("geometry", new JsonObject().put("type", "Point").put("coordinates", new JsonArray(List.of(8.5, 50.1)))))
        .collect(Collectors.toList()));
    String result = new SQLQuery("SELECT write_features(#{input}, 'FeatureCollection', 'deleter', true, #{version}, 'DELETE', "
        + "#{onNotExists}, NULL, NULL, false) AS result")
        .withNamedParameter("input", new JsonObject().put("type", "FeatureCollection").put("features", features).encode())
        .withNamedParameter("version", version)
        .withNamedParameter("onNotExists", onNotExists)
        .withContext(context)
        .run(dsp, rs -> rs.next() ? rs.getString("result") : null);
    return new JsonObject(result);
  }

  private List<Row> rows(String targetTable) throws SQLException {
    return new SQLQuery("""
        SELECT id, version, next_version = max_bigint() AS head, operation,
               (jsondata->'properties'->'@ns:com:here:xyz'->>'createdAt')::BIGINT AS created_at
          FROM ${schema}.${table}
         ORDER BY id, version
        """)
        .withVariable("schema", schema).withVariable("table", targetTable)
        .run(dsp, rs -> {
          List<Row> rows = new ArrayList<>();
          while (rs.next())
            rows.add(new Row(rs.getString("id"), rs.getLong("version"), rs.getBoolean("head"), rs.getString("operation"),
                rs.getLong("created_at")));
          return rows;
        });
  }

  private JsonObject deletedRowContent(String targetTable, String id) throws SQLException {
    return new SQLQuery("SELECT jsondata::TEXT AS jsondata FROM ${schema}.${table} WHERE id = #{id} AND operation = 'D'")
        .withVariable("schema", schema).withVariable("table", targetTable).withNamedParameter("id", id)
        .run(dsp, rs -> rs.next() ? new JsonObject(rs.getString("jsondata")) : null);
  }

  private record Row(String id, long version, boolean head, String operation, long createdAt) {}
}
