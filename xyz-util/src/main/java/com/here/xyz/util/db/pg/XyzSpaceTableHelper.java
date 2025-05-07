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

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;
import static com.here.xyz.models.hub.Space.TABLE_NAME;
import static com.here.xyz.util.db.pg.IndexHelper.buildCreateIndexQuery;
import static com.here.xyz.util.db.pg.IndexHelper.buildDropIndexQuery;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex.GEO;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex.VERSION_ID;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex.NEXT_VERSION;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex.OPERATION;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex.SERIAL;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex.VIZ;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SystemIndex.AUTHOR;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonValue;
import com.here.xyz.Typed;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.SQLQuery;
import org.apache.commons.codec.digest.DigestUtils;

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

  @JsonInclude(NON_DEFAULT)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonSubTypes({
      @JsonSubTypes.Type(value = SystemIndex.class, name = "SystemIndex"),
      @JsonSubTypes.Type(value = OnDemandIndex.class, name = "OnDemandIndex")
  })
  public interface Index extends Typed {
    String idxPrefix = "idx_";
    String getIndexName(String tableName);
  }

  public enum SystemIndex implements Index {
    GEO,
    VERSION_ID,
    NEXT_VERSION,
    OPERATION,
    SERIAL,
    VIZ,
    AUTHOR;

    @Override
    public String getIndexName(String tableName) {
      return switch (this) {
        case SERIAL, VIZ -> idxPrefix + tableName +  "_" + name().toLowerCase();
        case NEXT_VERSION -> idxPrefix + tableName +  "_nextversion";
        case VERSION_ID -> idxPrefix + tableName +  "_versionid";
        default -> idxPrefix + tableName + "_" + getIndexContent().get(0);
      };
    }

    public String getIndexType() {
        return switch (this) {
            case GEO -> "GIST";
            case VERSION_ID, NEXT_VERSION, OPERATION, SERIAL, VIZ, AUTHOR -> "BTREE";
        };
    }

    public List<String> getIndexContent() {
      return switch (this){
        case GEO -> List.of(("geo"));
        case VERSION_ID -> List.of("version", "id");
        case NEXT_VERSION -> List.of("next_version");
        case OPERATION -> List.of("operation");
        case SERIAL -> List.of("i");
        case VIZ -> List.of("(left(md5('' || i), 5))");
        case AUTHOR -> List.of("author");
      };
    }
  }

  public static class OnDemandIndex implements Index {
    private String propertyPath;

    public OnDemandIndex() { }

    public void setPropertyPath(String propertyPath) {
      this.propertyPath = propertyPath;
    }

    public String getPropertyPath() {
      return propertyPath;
    }

    public OnDemandIndex withPropertyPath(String propertyPath) {
      setPropertyPath(propertyPath);
      return this;
    }

    @Override
    public String getIndexName(String tableName) {
      // Take the first 8 characters of md5 hash of the property path
      String shortMd5 = DigestUtils.md5Hex(propertyPath).substring(0, 7);

      return idxPrefix + tableName + "_" + shortMd5 + "_m";
    }
  }

  public static SQLQuery buildSpaceTableIndexQuery(String schema, String table, Index index) {
    return buildCreateIndexQuery(schema, table, ((SystemIndex)index).getIndexContent(), ((SystemIndex)index).getIndexType(), index.getIndexName(table));
  }

  /**
   * @deprecated Please use only method {@link #buildSpaceTableIndexQueries(String, String)} instead.
   * @param schema
   * @param table
   * @param queryComment
   * @return
   */
  @Deprecated
  public static List<SQLQuery> buildSpaceTableIndexQueries(String schema, String table, SQLQuery queryComment) {
    return Arrays.asList(
        buildCreateIndexQuery(schema, table, GEO.getIndexContent(), GEO.getIndexType(), SystemIndex.GEO.getIndexName(table)),
        buildCreateIndexQuery(schema, table, VERSION_ID.getIndexContent(), VERSION_ID.getIndexType(), VERSION_ID.getIndexName(table)),
        buildCreateIndexQuery(schema, table, NEXT_VERSION.getIndexContent(), NEXT_VERSION.getIndexType(), NEXT_VERSION.getIndexName(table)),
        buildCreateIndexQuery(schema, table, OPERATION.getIndexContent(), OPERATION.getIndexType(), OPERATION.getIndexName(table)),
        buildCreateIndexQuery(schema, table, SERIAL.getIndexContent(), SERIAL.getIndexType(), SERIAL.getIndexName(table)),
        buildCreateIndexQuery(schema, table, VIZ.getIndexContent(), VIZ.getIndexType(), VIZ.getIndexName(table)),
        buildCreateIndexQuery(schema, table, AUTHOR.getIndexContent(), AUTHOR.getIndexType(), AUTHOR.getIndexName(table))
    ).stream().map(q -> addQueryComment(q, queryComment)).collect(Collectors.toList());
  }

  public static SQLQuery buildLoadSpaceTableIndicesQuery(String schema, String table) {
    return new SQLQuery("SELECT * FROM xyz_index_list_all_available(#{schema}, #{table});")
            .withNamedParameter("schema", schema)
            .withNamedParameter("table", table);
  }


  public static List<SQLQuery> buildSpaceTableDropIndexQueries(String schema, List<String> indices) {
    return indices.stream()
            .map(index -> buildDropIndexQuery(schema, index))
            .collect(Collectors.toList());
  }

  public static List<SQLQuery> buildCreateSpaceTableQueries(String schema, String table) {
    List<SQLQuery> queries = new ArrayList<>();

    queries.add(buildCreateSpaceTableQuery(schema, table));
    queries.add(buildColumnStorageAttributesQuery(schema, table));
    queries.addAll(buildSpaceTableIndexQueries(schema, table));
    queries.add(buildCreateHeadPartitionQuery(schema, table));
    queries.add(buildCreateHistoryPartitionQuery(schema, table, 0L));
    queries.add(buildCreateSequenceQuery(schema, table, "version"));

    return queries;
  }

  public static List<SQLQuery> buildSpaceTableIndexQueries(String schema, String table) {
    return buildSpaceTableIndexQueries(schema, table, null);
  }

  /**
   * @deprecated Please use labels instead. See: {@link SQLQuery#withLabel(String, String)}
   * @param sourceQuery
   * @param queryComment
   * @return
   */
  @Deprecated
  private static SQLQuery addQueryComment(SQLQuery sourceQuery, SQLQuery queryComment) {
    return queryComment != null ? sourceQuery.withQueryFragment("queryComment", queryComment) : sourceQuery;
  }

  public static SQLQuery buildColumnStorageAttributesQuery(String schema, String tableName) {
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
              + "i BIGSERIAL, "
              + "CONSTRAINT ${uniqueConstraintName} UNIQUE (id, next_version), "
              + "CONSTRAINT ${primKeyConstraintName} PRIMARY KEY (id, version, next_version)";

      SQLQuery createTable = new SQLQuery("CREATE TABLE IF NOT EXISTS ${schema}.${table} (${{tableFields}}) PARTITION BY RANGE (next_version)")
          .withQueryFragment("tableFields", tableFields)
          .withVariable(SCHEMA, schema)
          .withVariable(TABLE, table)
          .withVariable("uniqueConstraintName", table + "_unique")
          .withVariable("primKeyConstraintName", table + "_primKey");
      return createTable;
  }

  public static SQLQuery buildCreateSequenceQuery(String schema, String table, String columnName) {
      return new SQLQuery("CREATE SEQUENCE IF NOT EXISTS ${schema}.${sequence} MINVALUE 1 OWNED BY ${schema}.${table}.${columnName}")
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
