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

package com.here.xyz.util.db.pg;

import com.here.xyz.XyzSerializable;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.ConnectorParameters.TableLayout;
import com.here.xyz.util.db.SQLQuery;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.pg.IndexHelper.buildSpaceTableIndexQueries;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.V1;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.V2;

public class XyzSpaceTableHelper {

  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String HEAD_TABLE_SUFFIX = "_head";
  public static final long PARTITION_SIZE = 100_000;

  public static List<SQLQuery> buildCreateSpaceTableQueries(String schema, String table, String spaceId, TableLayout layout) {
    if (layout != TableLayout.V1 && layout != TableLayout.V2) {
      throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
    }

    List<SQLQuery> queries = new ArrayList<>();

    queries.add(buildCreateSpaceTableQuery(schema, table, layout));
    queries.add(buildAddTableCommentQuery(schema, table, new TableComment(spaceId, layout)));
    queries.add(buildColumnStorageAttributesQuery(schema, table, layout));
    queries.addAll(buildSpaceTableIndexQueries(schema, table, layout));
    queries.add(buildCreateHeadPartitionQuery(schema, table, layout));
    queries.add(buildCreateHistoryPartitionQuery(schema, table, 0L, layout));
    queries.add(buildCreateSequenceQuery(schema, table, "version", layout));

    return queries;
  }

  public static SQLQuery buildColumnStorageAttributesQuery(String schema, String tableName, TableLayout layout) {
    //Not needed for V2 layout
    if(layout == TableLayout.V1) {
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
    }else if(layout == TableLayout.V2) {
      return new SQLQuery("");
    }
    throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
  }

  public static SQLQuery buildCreateHeadPartitionQuery(String schema, String rootTable, TableLayout layout) {
    if(layout == TableLayout.V1)
      return new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${partitionTable} "
              + "PARTITION OF ${schema}.${rootTable} FOR VALUES FROM (max_bigint()) TO (MAXVALUE)")
              .withVariable(SCHEMA, schema)
              .withVariable("rootTable", rootTable)
              .withVariable("partitionTable", rootTable + HEAD_TABLE_SUFFIX);
    else if (layout == TableLayout.V2) {
      //Not needed for V2 layout
      return new SQLQuery("");
    }
    throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
  }

  public static SQLQuery buildCreateHistoryPartitionQuery(String schema, String rootTable, long partitionNo, TableLayout layout) {
    if(layout == TableLayout.V1)
      return new SQLQuery(
        "SELECT xyz_create_history_partition('" + schema + "', '" + rootTable + "', " + partitionNo + ", " + PARTITION_SIZE + ")");
    else if (layout == TableLayout.V2) {
      //TODO!
      return new SQLQuery("");
    }
    throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
  }

  public static SQLQuery buildCreateSpaceTableQuery(String schema, String table, TableLayout layout) {
    String tableFields = null;

    if(layout == V1) {
       tableFields = "id TEXT NOT NULL, "
              + "version BIGINT NOT NULL, "
              + "next_version BIGINT NOT NULL DEFAULT 9223372036854775807::BIGINT, "
              + "operation CHAR NOT NULL, "
              + "author TEXT, "
              + "jsondata JSONB, "
              + "geo geometry(GeometryZ, 4326), "
              + "i BIGSERIAL, "
              + "CONSTRAINT ${uniqueConstraintName} UNIQUE (id, next_version), "
              + "CONSTRAINT ${primKeyConstraintName} PRIMARY KEY (id, version, next_version)";
    } else if (layout == V2) {
      tableFields = "id TEXT NOT NULL, "
              + "version BIGINT NOT NULL, "
              + "next_version BIGINT NOT NULL DEFAULT 9223372036854775807::BIGINT, "
              + "operation CHAR NOT NULL, "
              + "author TEXT, "
              + "jsondata TEXT, "
              + "geo geometry(GeometryZ, 4326), "
              //+ "i BIGSERIAL, TODO: CHECK
              //+ "CONSTRAINT ${uniqueConstraintName} UNIQUE (id, next_version) " //TODO: CHECK
              + "CONSTRAINT ${primKeyConstraintName} PRIMARY KEY (id, version, next_version)";
    }

    return new SQLQuery(
        "CREATE TABLE IF NOT EXISTS ${schema}.${table} (${{tableFields}}) PARTITION BY RANGE (next_version)")
        .withQueryFragment("tableFields", tableFields)
        .withVariable(SCHEMA, schema)
        .withVariable(TABLE, table)
        .withVariable("uniqueConstraintName", table + "_unique")
        .withVariable("primKeyConstraintName", table + "_primKey");
  }

  public static SQLQuery buildCreateSequenceQuery(String schema, String table, String columnName, TableLayout layout) {
    if(layout == TableLayout.V1)
      return new SQLQuery("CREATE SEQUENCE IF NOT EXISTS ${schema}.${sequence} MINVALUE 1 OWNED BY ${schema}.${table}.${columnName}")
              .withVariable(SCHEMA, schema)
              .withVariable(TABLE, table)
              .withVariable("sequence", table + "_" + columnName + "_seq")
              .withVariable("columnName", columnName);
    else if (layout == TableLayout.V2) {
      //TODO!
      return new SQLQuery("");
    }
    throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
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

  public static SQLQuery buildAddTableCommentQuery(String schema, String table, TableComment tableComment) {
    return new SQLQuery("COMMENT ON TABLE ${schema}.${table} IS '${{tableComment}}'")
            .withVariable(SCHEMA, schema)
            .withVariable(TABLE, table)
            .withQueryFragment("tableComment", XyzSerializable.serialize(tableComment));
  }

  public static SQLQuery buildReadTableCommentQuery(String schema, String table) {
    return new SQLQuery("SELECT comment::JSON from obj_description( '${schema}.${table}'::regclass ) as comment;")
            .withVariable(SCHEMA, schema)
            .withVariable(TABLE, table);
  }

  public static List<SQLQuery>  buildCleanUpQuery(String schema, String table, String versionSequenceSuffix, TableLayout layout) {
    List<SQLQuery> queries = new ArrayList<>();

    SQLQuery dropTableQuery = new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table} ") //TODO: Why not use CASCADE
            .withVariable(SCHEMA, schema)
            .withVariable(TABLE, table);
    SQLQuery dropVersionSequenceQuery = new SQLQuery("DROP SEQUENCE IF EXISTS ${schema}.${versionSequence};")
            .withVariable("versionSequence", table + versionSequenceSuffix)
            .withVariable(SCHEMA, schema);

    if(layout == TableLayout.V1) {
      // Gets removed
      SQLQuery deleteSpaceMetaEntry = new SQLQuery("DELETE FROM xyz_config.space_meta WHERE h_id = #{table} AND schem = #{schema};")
              .withNamedParameter(SCHEMA, schema)
              .withNamedParameter(TABLE, table);
      // Gets removed
      SQLQuery deleteIndexStatusEntry = new SQLQuery("DELETE FROM xyz_config.xyz_idxs_status WHERE spaceid = #{table} AND schem = #{schema};")
                .withNamedParameter(SCHEMA, schema)
                .withNamedParameter(TABLE, table);

      SQLQuery dropISequenceQuery = new SQLQuery("DROP SEQUENCE IF EXISTS ${schema}.${iSequence};")
              .withVariable("iSequence", table + "_i_seq") //Assuming iSequence suffix is "_i_seq"
              .withVariable(SCHEMA, schema);

      // Gets removed
      queries.add(deleteSpaceMetaEntry);
      queries.add(deleteIndexStatusEntry);
      // Gets removed
      queries.add(dropTableQuery);
      queries.add(dropISequenceQuery);
      queries.add(dropVersionSequenceQuery);
      return queries;
    }else if(layout == TableLayout.V2) {
      queries.add(dropTableQuery);
      queries.add(dropVersionSequenceQuery);
    }

    throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
  }

  public record TableComment(String spaceId, TableLayout tableLayout) implements XyzSerializable{
  }
}
