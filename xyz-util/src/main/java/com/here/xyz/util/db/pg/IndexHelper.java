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
import com.here.xyz.Typed;

import com.here.xyz.util.db.ConnectorParameters.TableLayout;
import com.here.xyz.util.db.SQLQuery;
import org.apache.commons.codec.digest.DigestUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.V1;
import static com.here.xyz.util.db.ConnectorParameters.TableLayout.V2;

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
      return switch (this) {
        case GEO -> List.of("geo");
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
            .map(entry -> new XyzSpaceTableHelper.OnDemandIndex().withPropertyPath(entry.getKey()))
            .collect(Collectors.toList());
  }

  public static SQLQuery buildOnDemandIndexCreationQuery(String schema, String table, String propertyPath, boolean async){
    return new SQLQuery((async ? "PERFORM " : "SELECT ") +
            """
            xyz_index_creation_on_property_object(
                #{schema_name},
                #{table_name},
                #{property_name},
                xyz_index_name_for_property(#{table_name}, #{property_name}, #{idx_type}),
                xyz_property_datatype(#{schema_name}, #{table_name}, #{property_name}, #{table_sample_cnt}),
                #{idx_type}
              )
            """)
            .withNamedParameter("schema_name", schema)
            .withNamedParameter("table_name", table)
            .withNamedParameter("property_name", propertyPath)
            .withNamedParameter("table_sample_cnt", 5000)
            .withNamedParameter("idx_type", "m");
  }

  public static SQLQuery checkIndexType(String schema, String table, String propertyName, int tableSampleCnt) {
    return new SQLQuery("""
        SELECT * FROM xyz_index_creation_on_property_object( #{schema_name}, #{table_name},
            #{property_name}, #{table_sample_cnt} )
        """)
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
    if(layout == V1)
      return Arrays.asList(SystemIndex.values()).stream()
              .map(index -> buildCreateIndexQuery(schema, table, index.getIndexContent(), index.getIndexType(), index.getIndexName(table)))
              .toList();
    else if (layout == V2)
      return List.of(new SQLQuery(""));

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
    return buildSpaceTableIndexQueries(schema, table, TableLayout.V1)
            .stream()
            .map(q -> addQueryComment(q, queryComment))
            .toList();
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
