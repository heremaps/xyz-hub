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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.Typed;

import com.here.xyz.util.db.ConnectorParameters.TableLayout;
import com.here.xyz.util.db.SQLQuery;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.OLD_LAYOUT;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.NEW_LAYOUT;

public class IndexHelper {

  @JsonInclude(NON_DEFAULT)
  @JsonIgnoreProperties(ignoreUnknown = true)
  @JsonSubTypes({
          @JsonSubTypes.Type(value = SystemIndex.class, name = "SystemIndex"),
          @JsonSubTypes.Type(value = OnDemandIndex.class, name = "OnDemandIndex")
  })
  public interface Index extends Typed {
    String idxPrefix = "idx_";
    String getIndexName(String tableName);
    String getIndexName();
  }

  @JsonTypeName("SystemIndex")
  public enum SystemIndex implements Index {
    GEO,
    VERSION_ID,
    NEXT_VERSION,
    OPERATION,
    SERIAL,
    VIZ,
    AUTHOR;

    String indexName;

    public String getIndexName() {
      return indexName;
    }

    @Override
    public String getIndexName(String tableName) {
      switch (this) {
        case SERIAL:
        case VIZ:
          return idxPrefix + tableName +  "_" + name().toLowerCase();
        case NEXT_VERSION:
          return idxPrefix + tableName +  "_nextversion";
        case VERSION_ID:
          return idxPrefix + tableName +  "_versionid";
        default:
          return idxPrefix + tableName + "_" + getIndexContent().get(0);
      }
    }

    public String getIndexType() {
      switch (this) {
        case GEO:
          return "GIST";
        case VERSION_ID:
        case NEXT_VERSION:
        case OPERATION:
        case SERIAL:
        case VIZ:
        case AUTHOR:
          return "BTREE";
        default:
          throw new IllegalStateException("Unexpected index type: " + this);
      }
    }

    public List<String> getIndexContent() {
      switch (this) {
        case GEO:
          return List.of("geo");
        case VERSION_ID:
          return List.of("version", "id");
        case NEXT_VERSION:
          return List.of("next_version");
        case OPERATION:
          return List.of("operation");
        case SERIAL:
          return List.of("i");
        case VIZ:
          return List.of("(left(md5('' || i), 5))");
        case AUTHOR:
          return List.of("author");
        default:
          throw new IllegalStateException("Unexpected index: " + this);
      }
    }

    public static SystemIndex fromString(String name) {
      if (name == null) return null;
      switch (name.toLowerCase()) {
        case "geo":
          return GEO;
        case "versionid":
          return VERSION_ID;
        case "nextversion":
          return NEXT_VERSION;
        case "operation":
          return OPERATION;
        case "serial":
          return SERIAL;
        case "viz":
          return VIZ;
        case "author":
          return AUTHOR;
        default:
          return null;
      }
    }
  }

  @JsonTypeName("OnDemandIndex")
  public static class OnDemandIndex implements Index {
    private String indexName;
    private String propertyPath;

    public OnDemandIndex() { }

    public OnDemandIndex(String indexName) {
      this.indexName = indexName;
    }

    public String getIndexName() {
      return indexName;
    }

    public OnDemandIndex withIndexName(String indexName) {
      this.indexName = indexName;
      return this;
    }

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

  public static SQLQuery buildCreateIndexQuery(String schema, String table, String columnName, String method) {
    return buildCreateIndexQuery(schema, table, Collections.singletonList(columnName), method);
  }

  public static SQLQuery buildCreateIndexQuery(String schema, String table, Index index) {
    return buildCreateIndexQuery(schema, table, ((SystemIndex)index).getIndexContent(), ((SystemIndex)index).getIndexType(), index.getIndexName(table));
  }

  public static SQLQuery buildCreateIndexQuery(String schema, String table, List<String> columnNames, String method) {
    return buildCreateIndexQuery(schema, table, columnNames, method, "idx_" + table + "_"
        + columnNames
        .stream()
        .map(colName -> colName.replace("_", ""))
        .collect(Collectors.joining()), null);
  }

  public static SQLQuery buildCreateIndexQuery(String schema, String table, String columnNameOrExpression, String method,
      String indexName) {
      return buildCreateIndexQuery(schema, table, Collections.singletonList(columnNameOrExpression), method, indexName, null);
  }

  public static SQLQuery buildSpaceTableIndexQuery(String schema, String table, Index index) {
    return buildCreateIndexQuery(schema, table, ((SystemIndex)index).getIndexContent(), ((SystemIndex)index).getIndexType(), index.getIndexName(table));
  }

  public static SQLQuery buildCreateIndexQuery(String schema, String table, List<String> columnNamesOrExpressions, String method,
      String indexName) {
    return buildCreateIndexQuery(schema, table, columnNamesOrExpressions, method, indexName, null);
  }

  private static SQLQuery buildCreateIndexQuery(String schema, String table, List<String> columnNamesOrExpressions, String method,
      String indexName, String predicate) {
    return new SQLQuery("CREATE INDEX ${{queryComment}} IF NOT EXISTS ${indexName} ON ${schema}.${table} USING " + method
        + " (" + String.join(", ", columnNamesOrExpressions) + ") ${{predicate}}")
        .withVariable("schema", schema)
        .withVariable("table", table)
        .withVariable("indexName", indexName)
        .withQueryFragment("predicate", predicate != null ? "WHERE " + predicate : "")
        .withQueryFragment("queryComment", "");
  }

  public static SQLQuery buildDropIndexQuery(String schema, String indexName) {
    return new SQLQuery("DROP INDEX ${{queryComment}} IF EXISTS ${schema}.${indexName} CASCADE")
        .withVariable("schema", schema)
        .withVariable("indexName", indexName)
        .withQueryFragment("queryComment", "");
  }

  public static List<OnDemandIndex> getActivatedSearchableProperties(Map<String, Boolean> searchableProperties) {
    return searchableProperties == null ? List.of() : searchableProperties.entrySet().stream()
            .filter(Map.Entry::getValue)
            .map(entry -> new OnDemandIndex().withPropertyPath(entry.getKey()))
            .collect(Collectors.toList());
  }

  public static SQLQuery buildOnDemandIndexCreationQuery(String schema, String table, String propertyPath, boolean async){
    return buildOnDemandIndexCreationQuery(schema, table, propertyPath, "jsondata", async);
  }

  public static SQLQuery buildOnDemandIndexCreationQuery(String schema, String table, String propertyPath, String targetColumn, boolean async){
    return new SQLQuery((async ? "PERFORM " : "SELECT ") +
            "xyz_index_creation_on_property_object(\n" +
            "    #{schema_name},\n" +
            "    #{table_name},\n" +
            "    #{property_name},\n" +
            "    xyz_index_name_for_property(#{table_name}, #{property_name}, #{idx_type}),\n" +
            "    xyz_property_datatype(#{schema_name}, #{table_name}, #{property_name}, #{table_sample_cnt}, #{target_column} ),\n" +
            "    #{idx_type},\n" +
            "    #{target_column}\n" +
            "  )\n")
            .withNamedParameter("schema_name", schema)
            .withNamedParameter("table_name", table)
            .withNamedParameter("property_name", propertyPath)
            .withNamedParameter("table_sample_cnt", 5000)
            .withNamedParameter("idx_type", "m")
            .withNamedParameter("target_column", targetColumn);
  }

  public static SQLQuery checkIndexType(String schema, String table, String propertyName, int tableSampleCnt) {
    return new SQLQuery(
        "SELECT * FROM xyz_index_creation_on_property_object( #{schema_name}, #{table_name},\n" +
        "    #{property_name}, #{table_sample_cnt} )\n")
        .withNamedParameter("schema_name", schema)
        .withNamedParameter("table_name", table)
        .withNamedParameter("property_name", propertyName)
        .withNamedParameter("table_sample_cnt", tableSampleCnt);
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

  public static List<SQLQuery> buildSpaceTableIndexQueries(String schema, String table, TableLayout layout) {
    if(layout == OLD_LAYOUT)
      return Arrays.asList(SystemIndex.values()).stream()
              .map(index -> buildCreateIndexQuery(schema, table, index.getIndexContent(), index.getIndexType(), index.getIndexName(table)))
              .collect(Collectors.toList());
    else if (layout == NEW_LAYOUT)
      return Stream.of(SystemIndex.GEO,
                      SystemIndex.NEXT_VERSION,
                      SystemIndex.VERSION_ID)
              .map(index -> buildCreateIndexQuery(
                      schema, table, index.getIndexContent(),
                      index.getIndexType(), index.getIndexName(table))
              ).collect(Collectors.toList());

    throw new IllegalArgumentException("Unsupported layout " + layout);
  }

  /**
   * @deprecated Please use only method {@link #buildSpaceTableIndexQueries(String, String, TableLayout)} instead.
   * @param schema
   * @param table
   * @param queryComment
   * @return
   */
  @Deprecated
  public static List<SQLQuery> buildSpaceTableIndexQueries(String schema, String table, SQLQuery queryComment) {
    return buildSpaceTableIndexQueries(schema, table, TableLayout.OLD_LAYOUT)
            .stream()
            .map(q -> addQueryComment(q, queryComment))
            .collect(Collectors.toList());
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
}
