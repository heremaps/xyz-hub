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

package com.here.xyz.util.db.pg;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.pg.IndexHelper.buildCreateIndexQuery;

import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.SQLQuery;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class XyzSpaceTableHelper {

  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String HEAD_TABLE_SUFFIX = "_head";
  public static final long PARTITION_SIZE = 100_000;

  public static List<SQLQuery> buildSpaceTableIndexQueries(String schema, String table, SQLQuery queryComment) {
    return Arrays.asList(
        buildCreateIndexQuery(schema, table, Arrays.asList("id", "version"), "BTREE"),
        buildCreateIndexQuery(schema, table, "geo", "GIST"),
        buildCreateIndexQuery(schema, table, "id", "BTREE", "idx_" + table + "_idnew"),
        buildCreateIndexQuery(schema, table, "version", "BTREE"),
        buildCreateIndexQuery(schema, table, "next_version", "BTREE"),
        buildCreateIndexQuery(schema, table, "operation", "BTREE"),
        //buildCreateIndexQuery(schema, table, "(jsondata->'properties'->'@ns:com:here:xyz'->'tags') jsonb_ops", "GIN", "idx_" + table + "_tags"),
        buildCreateIndexQuery(schema, table, "i", "BTREE", "idx_" + table + "_serial"),
        buildCreateIndexQuery(schema, table, Arrays.asList("(jsondata->'properties'->'@ns:com:here:xyz'->'updatedAt')", "id"), "BTREE", "idx_" + table + "_updatedAt"),
        buildCreateIndexQuery(schema, table, Arrays.asList("(jsondata->'properties'->'@ns:com:here:xyz'->'createdAt')", "id"), "BTREE", "idx_" + table + "_createdAt"),
        buildCreateIndexQuery(schema, table, "(left(md5('' || i), 5))", "BTREE", "idx_" + table + "_viz"),
        buildCreateIndexQuery(schema, table, "author", "BTREE")
    ).stream().map(q -> addQueryComment(q, queryComment)).collect(Collectors.toList());
  }

  public static List<SQLQuery> buildCreateSpaceTableQueries(String schema, String table) {
    List<SQLQuery> queries = new ArrayList<>();

    queries.add(buildCreateSpaceTableQuery(schema, table));
    queries.add(buildColumnStoragAttributesQuery(schema, table));
    queries.addAll(buildSpaceTableIndexQueries(schema, table));
    queries.add(buildCreateHeadPartitionQuery(schema, table));
    queries.add(buildCreateHistoryPartitionQuery(schema, table, 0L));
    queries.add(buildCreateSequenceQuery(schema, table, "version"));

    return queries;
  }

  public static List<SQLQuery> buildSpaceTableIndexQueries(String schema, String table) {
    return buildSpaceTableIndexQueries(schema, table, null);
  }

  private static SQLQuery addQueryComment(SQLQuery indexCreationQuery, SQLQuery queryComment) {
    return queryComment != null ? indexCreationQuery.withQueryFragment("queryComment", queryComment) : indexCreationQuery;
  }

  public static SQLQuery buildColumnStoragAttributesQuery(String schema, String tableName) {
      return new SQLQuery("ALTER TABLE ${schema}.${table} "
          + "ALTER COLUMN id SET STORAGE MAIN, "
          + "ALTER COLUMN jsondata SET STORAGE MAIN, "
          + "ALTER COLUMN geo SET STORAGE MAIN, "
          + "ALTER COLUMN operation SET STORAGE PLAIN, "
          + "ALTER COLUMN next_version SET STORAGE PLAIN, "
          + "ALTER COLUMN version SET STORAGE PLAIN, "
          + "ALTER COLUMN i SET STORAGE PLAIN, "
          + "ALTER COLUMN author SET STORAGE MAIN, "

          + "ALTER COLUMN id SET COMPRESSION lz4, "
          + "ALTER COLUMN jsondata SET COMPRESSION lz4, "
          + "ALTER COLUMN geo SET COMPRESSION lz4, "
          + "ALTER COLUMN author SET COMPRESSION lz4;")
          .withVariable(SCHEMA, schema)
          .withVariable(TABLE, tableName);
  }

  public static SQLQuery buildCreateHeadPartitionQuery(String schema, String rootTable) {
      return new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${partitionTable} "
          + "PARTITION OF ${schema}.${rootTable} FOR VALUES FROM (max_bigint()) TO (MAXVALUE)")
          .withVariable(SCHEMA, schema)
          .withVariable("rootTable", rootTable)
          .withVariable("partitionTable", rootTable + HEAD_TABLE_SUFFIX);
  }

  public static SQLQuery buildCreateHistoryPartitionQuery(String schema, String rootTable, long partitionNo) {
      return new SQLQuery(
          "SELECT xyz_create_history_partition('" + schema + "', '" + rootTable + "', " + partitionNo + ", " + PARTITION_SIZE + ")");
  }

  public static SQLQuery buildCreateSpaceTableQuery(String schema, String table) {
      String tableFields = "id TEXT NOT NULL, "
              + "version BIGINT NOT NULL, "
              + "next_version BIGINT NOT NULL DEFAULT 9223372036854775807::BIGINT, "
              + "operation CHAR NOT NULL, "
              + "author TEXT, "
              + "jsondata JSONB, "
              + "geo geometry(GeometryZ, 4326), "
              + "i BIGSERIAL"
              + ", CONSTRAINT ${constraintName} PRIMARY KEY (id, version, next_version)";

      SQLQuery createTable = new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (${{tableFields}}) PARTITION BY RANGE (next_version)")
          .withQueryFragment("tableFields", tableFields)
          .withVariable(SCHEMA, schema)
          .withVariable(TABLE, table)
          .withVariable("constraintName", table + "_primKey");
      return createTable;
  }

  public static SQLQuery buildCreateSequenceQuery(String schema, String table, String columnName) {
      return new SQLQuery("CREATE SEQUENCE IF NOT EXISTS ${schema}.${sequence} MINVALUE 0 OWNED BY ${schema}.${table}.${columnName}")
          .withVariable(SCHEMA, schema)
          .withVariable(TABLE, table)
          .withVariable("sequence", table + "_" + columnName + "_seq")
          .withVariable("columnName", columnName);
  }

  public static String getTableNameForSpaceId(String spaceId, boolean hashed) {
    if (spaceId == null || spaceId.length() == 0)
      return null;

    return hashed ? Hasher.getHash(spaceId) : spaceId;
  }

  public static String getTableNameFromSpaceParamsOrSpaceId(Map<String, Object> spaceParams, String spaceId, boolean hashed) {
    if (spaceParams != null) {
      Object tableName = spaceParams.get(TABLE_NAME);
      if (tableName instanceof String && ((String) tableName).length() > 0)
        return (String) tableName;
    }

    return getTableNameForSpaceId(spaceId, hashed);
  }
}
