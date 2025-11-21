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

import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.OLD_LAYOUT;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.NEW_LAYOUT;
import static com.here.xyz.util.db.pg.IndexHelper.OnDemandIndex;
import static com.here.xyz.util.db.pg.IndexHelper.buildSpaceTableIndexQueries;
import static com.here.xyz.util.db.pg.IndexHelper.buildOnDemandIndexCreationQuery;

import com.here.xyz.XyzSerializable;
import com.here.xyz.events.ModifySpaceEvent;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.ConnectorParameters.TableLayout;
import com.here.xyz.util.db.SQLQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XyzSpaceTableHelper {

  public static final String SCHEMA = "schema";
  public static final String TABLE = "table";
  public static final String HEAD_TABLE_SUFFIX = "_head";
  public static final String SEARCHABLE_COLUMN = "searchable";
  public static final String REF_QUAD_PROPERTY_KEY = "refQuad";
  public static final String GLOBAL_VERSION_PROPERTY_KEY = "globalVersion";
  public static final String REFERENCES_PROPERTY_KEY = "references";
  public static final long HEAD_TABLE_PARTION_COUNT = 10;
  public static final long PARTITION_SIZE = 100_000;

  public static List<SQLQuery> buildCreateSpaceTableQueriesForTests(String schema, String table) {
    //In test`s we spaceId is always the same as table name
    return buildCreateSpaceTableQueries(schema, table, List.of(), table, OLD_LAYOUT);
  }
  public static List<SQLQuery> buildCreateSpaceTableQueries(String schema, String table, String spaceId) {
    return buildCreateSpaceTableQueries(schema, table, List.of(), spaceId, OLD_LAYOUT);
  }

  public static List<SQLQuery> buildCreateSpaceTableQueries(String schema, String table, String spaceId, TableLayout layout) {
    return buildCreateSpaceTableQueries(schema, table, List.of(), spaceId, layout);
  }

  public static List<SQLQuery> buildCreateSpaceTableQueries(String schema, String table, List<OnDemandIndex> onDemandIndices,
      String spaceId, TableLayout layout) {
    return buildCreateSpaceTableQueries(schema, table, true, onDemandIndices, spaceId, layout);
  }

  //TODO: Check from where to get the on-demand index info in case of branch creations (see onDemandIndices param below)
  public static List<SQLQuery> buildCreateBranchTableQueries(String schema, String table, String spaceId) {
    return buildCreateSpaceTableQueries(schema, table, false, List.of(), spaceId);
  }

  public static List<SQLQuery> buildCreateSpaceTableQueries(String schema, String table, boolean isMainTable,  List<OnDemandIndex> onDemandIndices,
                                                            String spaceId) {
    return buildCreateSpaceTableQueries(schema, table, isMainTable, onDemandIndices, spaceId, OLD_LAYOUT);
  }

  public static List<SQLQuery> buildCreateSpaceTableQueries(String schema, String table, boolean isMainTable,  List<OnDemandIndex> onDemandIndices,
                                                            String spaceId, TableLayout layout) {
    if (layout != TableLayout.OLD_LAYOUT && layout != TableLayout.NEW_LAYOUT) {
      throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
    }

    List<SQLQuery> queries = new ArrayList<>();
    queries.add(createConnectorSchema(schema));
    queries.add(buildCreateSpaceTableQuery(schema, table, layout));
    queries.add(buildAddTableCommentQuery(schema, table, new TableComment(spaceId, layout)));
    queries.add(buildColumnStorageAttributesQuery(schema, table, layout));
    queries.addAll(buildSpaceTableIndexQueries(schema, table, layout));
    queries.addAll(buildCreateHeadPartitionQuery(schema, table, layout));
    queries.add(buildCreateHistoryPartitionQuery(schema, table, 0L, true));
    queries.add(buildCreateSequenceQuery(schema, table, "version", layout));
    if(onDemandIndices != null && !onDemandIndices.isEmpty()) {
        for (OnDemandIndex onDemandIndex : onDemandIndices)
            queries.add(buildOnDemandIndexCreationQuery(schema, table, onDemandIndex.getPropertyPath(), false));
    }
    if(layout == NEW_LAYOUT) {
      queries.add(buildOnDemandIndexCreationQuery(schema, table, REF_QUAD_PROPERTY_KEY, SEARCHABLE_COLUMN ,false));
      queries.add(buildOnDemandIndexCreationQuery(schema, table, GLOBAL_VERSION_PROPERTY_KEY, SEARCHABLE_COLUMN , false));
      queries.add(buildOnDemandIndexCreationQuery(schema, table, REFERENCES_PROPERTY_KEY + "::ARRAY", SEARCHABLE_COLUMN , false));
    }
    if (isMainTable)
      queries.add(buildCreateBranchSequenceQuery(schema, table));

    return queries;
  }

  public static SQLQuery buildColumnStorageAttributesQuery(String schema, String tableName, TableLayout layout) {
    //Not needed for V2 layout
    if(layout == TableLayout.OLD_LAYOUT) {
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
    }else if(layout == TableLayout.NEW_LAYOUT) {
      //Not needed for V2 layout
      return new SQLQuery("");
    }
    throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
  }

  public static List<SQLQuery>  buildCreateHeadPartitionQuery(String schema, String rootTable, TableLayout layout) {
    if(layout == TableLayout.OLD_LAYOUT)
      return List.of(new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${partitionTable} "
              + "PARTITION OF ${schema}.${rootTable} FOR VALUES FROM (max_bigint()) TO (MAXVALUE)")
              .withVariable(SCHEMA, schema)
              .withVariable("rootTable", rootTable)
              .withVariable("partitionTable", rootTable + HEAD_TABLE_SUFFIX));
    else if (layout == TableLayout.NEW_LAYOUT) {
      SQLQuery headTableCreation = new SQLQuery(
              "CREATE TABLE IF NOT EXISTS ${schema}.${partitionTable}\n" +
              "  PARTITION OF ${schema}.${rootTable} FOR VALUES FROM (max_bigint()) TO (MAXVALUE)\n" +
              "  PARTITION BY HASH (id);\n")
              .withVariable(SCHEMA, schema)
              .withVariable("rootTable", rootTable)
              .withVariable("partitionTable", rootTable + HEAD_TABLE_SUFFIX);

      SQLQuery headTablePartitionCreations = new SQLQuery(
              ("DO $$\n" +
              "BEGIN\n" +
              "  FOR i IN 0..$partitionCountLoop$ LOOP\n" +
              "    EXECUTE format('\n" +
              "      CREATE TABLE IF NOT EXISTS $schema$.\"$headTable$_p%s\"\n" +
              "        PARTITION OF $schema$.\"$headTable$\"\n" +
              "        FOR VALUES WITH (MODULUS $partitionCount$, REMAINDER %s);', i, i);\n" +
              "  END LOOP;\n" +
              "END $$;\n")
              .replace("$partitionCountLoop$", Long.toString(HEAD_TABLE_PARTION_COUNT - 1))
              .replace("$schema$",schema)
              .replace("$headTable$",rootTable + HEAD_TABLE_SUFFIX)
              .replace("$partitionCount$",Long.toString(HEAD_TABLE_PARTION_COUNT))
      );
      return List.of(
              headTableCreation,
              headTablePartitionCreations
      );
    }
    throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
  }

  public static SQLQuery buildCreateHistoryPartitionQuery(String schema, String rootTable, long partitionNo, boolean useSelect) {
    return new SQLQuery((useSelect ? "SELECT" : "PERFORM") + " xyz_create_history_partition('" + schema + "', '" + rootTable + "', " + partitionNo + ", " + PARTITION_SIZE + ")");
  }

  public static SQLQuery createConnectorSchema(String schema) {
        return new SQLQuery("CREATE SCHEMA IF NOT EXISTS ${schema}")
                .withVariable(SCHEMA, schema);
  }

  private static SQLQuery buildCreateSpaceTableQuery(String schema, String table, TableLayout layout) {
    String tableFields = null;

    if(layout == OLD_LAYOUT) {
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
    } else if (layout == NEW_LAYOUT) {
      tableFields = "id TEXT STORAGE MAIN COMPRESSION lz4 NOT NULL, "
              + "version BIGINT STORAGE PLAIN NOT NULL, "
              + "next_version BIGINT STORAGE PLAIN NOT NULL DEFAULT 9223372036854775807::BIGINT, "
              + "operation CHAR STORAGE PLAIN NOT NULL, "
              + "author TEXT STORAGE MAIN COMPRESSION lz4, "
              + "jsondata TEXT STORAGE MAIN COMPRESSION lz4 , "
              + "geo geometry(GeometryZ, 4326) STORAGE MAIN COMPRESSION lz4, "
              + "searchable JSONB STORAGE MAIN COMPRESSION lz4,"
              + "i BIGSERIAL, "
//              + "CONSTRAINT ${uniqueConstraintName} UNIQUE (id, next_version), "
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
    if(layout == TableLayout.OLD_LAYOUT || layout == TableLayout.NEW_LAYOUT)
      return buildCreateSequenceQuery(schema, table, columnName, columnName, 1);
    throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
  }

  public static SQLQuery buildCreateBranchSequenceQuery(String schema, String table) {
    return buildCreateSequenceQuery(schema, table, "branches", "id", 1);
  }

  public static String sequenceName(String table, String sequenceName) {
    return table + "_" + sequenceName + "_seq";
  }

  public static SQLQuery buildCreateSequenceQuery(String schema, String table, String sequenceName, String ownedByColumn, int startValue) {
      return new SQLQuery("CREATE SEQUENCE IF NOT EXISTS ${schema}.${sequence} MINVALUE #{startValue} OWNED BY ${schema}.${table}.${columnName}")
              .withVariable(SCHEMA, schema)
              .withVariable(TABLE, table)
              .withVariable("sequence", sequenceName(table, sequenceName))
              .withVariable("columnName", ownedByColumn)
          .withNamedParameter("startValue", startValue);
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

  public static List<SQLQuery>  buildCleanUpQuery(ModifySpaceEvent event, String schema, String table, String versionSequenceSuffix, TableLayout layout) {
    List<SQLQuery> queries = new ArrayList<>();

    SQLQuery dropTableQuery = new SQLQuery("DROP TABLE IF EXISTS ${schema}.${table} ") //TODO: Why not use CASCADE
            .withVariable(SCHEMA, schema)
            .withVariable(TABLE, table);
    SQLQuery dropVersionSequenceQuery = new SQLQuery("DROP SEQUENCE IF EXISTS ${schema}.${versionSequence};")
            .withVariable("versionSequence", table + versionSequenceSuffix)
            .withVariable(SCHEMA, schema);

    if(layout == TableLayout.OLD_LAYOUT) {
      //MMSUP-1092  tmp workaroung on db9 - skip deletion from spaceMetaTable
      //TODO: remove spaceMetaTable from overall code
      String storageID = event.getSpaceDefinition() != null && event.getSpaceDefinition().getStorage() != null
                      ? event.getSpaceDefinition().getStorage().getId()
                      : "no-connector-info-available";
      //Remove also event as parameter and cleanUp deleteSpaceMetaEntry
      //MMSUP-1092

      // Gets removed
      SQLQuery deleteSpaceMetaEntry = "psql-db9-eu-west-1".equals(storageID) ? new SQLQuery("") :
              new SQLQuery("DELETE FROM xyz_config.space_meta WHERE h_id = #{table} AND schem = #{schema};")
              .withNamedParameter(SCHEMA, schema)
              .withNamedParameter(TABLE, table);

      SQLQuery dropISequenceQuery = new SQLQuery("DROP SEQUENCE IF EXISTS ${schema}.${iSequence};")
              .withVariable("iSequence", table + "_i_seq") //Assuming iSequence suffix is "_i_seq"
              .withVariable(SCHEMA, schema);

      // Gets removed
      queries.add(deleteSpaceMetaEntry);
      // Gets removed
      queries.add(dropTableQuery);
      queries.add(dropISequenceQuery);
      queries.add(dropVersionSequenceQuery);
      return queries;
    }else if(layout == TableLayout.NEW_LAYOUT) {
      queries.add(dropTableQuery);
      queries.add(dropVersionSequenceQuery);
      return queries;
    }

    throw new IllegalArgumentException("Unsupported Table Layout: " + layout);
  }

  public static class TableComment implements XyzSerializable {
    private final String spaceId;
    private final TableLayout tableLayout;

    public TableComment(String spaceId, TableLayout tableLayout) {
      this.spaceId = spaceId;
      this.tableLayout = tableLayout;
    }

    public String getSpaceId() {
      return spaceId;
    }

    public TableLayout getTableLayout() {
      return tableLayout;
    }
  }
}
