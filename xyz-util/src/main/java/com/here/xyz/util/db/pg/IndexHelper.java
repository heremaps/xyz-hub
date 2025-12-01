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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_DEFAULT;

public class IndexHelper {
  private static String OLD_LAYOUT_INDEX_COULMN = "jsondata";
  private static String NEW_LAYOUT_INDEX_COULMN = "searchable";

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

    public static SystemIndex fromString(String name) {
      if (name == null) return null;
      return switch (name.toLowerCase()) {
        case "geo" -> GEO;
        case "versionid" -> VERSION_ID;
        case "nextversion" -> NEXT_VERSION;
        case "operation" -> OPERATION;
        case "serial" -> SERIAL;
        case "viz" -> VIZ;
        case "author" -> AUTHOR;
        default -> null;
      };
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
      transformIntoNewDefinition();
    }

    public String getPropertyPath() {
      return propertyPath;
    }

    public OnDemandIndex withPropertyPath(String propertyPath) {
      setPropertyPath(propertyPath);
      return this;
    }

    public String extractAlias(){
      if (propertyPath == null)
        return null;
      if (propertyPath.startsWith("$"))
        return propertyPath.substring(1, propertyPath.indexOf(':'));
      throw new IllegalArgumentException("Cannot extract alias from property path: " + propertyPath);
    }

    public String extractLogicalPropertyPath() {
      if (propertyPath == null)
        return null;

      propertyPath = propertyPath.trim();

      // New-style: $alias:[$.jsonPath]::scalar|array
      if (propertyPath.startsWith("$") && propertyPath.contains("::")) {
        String[] typeSplit = propertyPath.split("::", 2);
        String leftPart = typeSplit[0].trim();

        int colonIdx = leftPart.indexOf(':');
        String exprPart = colonIdx > -1
                ? leftPart.substring(colonIdx + 1).trim()
                : leftPart.substring(1).trim();

        // Strip [] if present
        if (exprPart.startsWith("[") && exprPart.endsWith("]") && exprPart.length() > 2) {
          exprPart = exprPart.substring(1, exprPart.length() - 1).trim();
        }

        if (exprPart.startsWith("$.") && exprPart.length() > 2) {
          return exprPart.substring(2);
        }
        if (exprPart.startsWith("$") && exprPart.length() > 1) {
          return exprPart.substring(1); // fallback
        }
        return exprPart;
      }

      // Legacy keys
      int sepIdx = propertyPath.lastIndexOf("::");
      if (sepIdx > -1) {
        return propertyPath.substring(0, sepIdx).trim();
      }

      return propertyPath;
    }

    public void transformIntoNewDefinition() {
      //if already in new format, return as is
      if(propertyPath.startsWith("$"))
        return;

      // Trim whitespace
      propertyPath = propertyPath.trim();

      String path = propertyPath;
      String datatype = "scalar"; // default

      // Check if datatype explicitly provided
      int idx = propertyPath.indexOf("::");
      if (idx >= 0) {
        path = propertyPath.substring(0, idx);
        String dt = propertyPath.substring(idx + 2).trim();
        if ("array".equalsIgnoreCase(dt)) {
          datatype = "array";
        }
      }

      String alias = path;
      if(!path.startsWith("f."))
        alias = path = "properties."+path;
      else
        path = path.substring("f.".length());

      propertyPath = "$" + alias + ":[$." + path + "]::" + datatype;
    }

    public boolean definitionGotTransformed() {
      String alias = extractAlias();
      return alias.startsWith("f.") || alias.startsWith("properties.");
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
    return searchableProperties == null
            ? List.of()
            : searchableProperties.entrySet().stream()
            .filter(Map.Entry::getValue)
            .map(entry -> new OnDemandIndex()
                    .withPropertyPath(entry.getKey()))
            .collect(Collectors.toList());
  }

  public static SQLQuery buildOnDemandIndexCreationQuery(String schema, String table, OnDemandIndex index, TableLayout layout, boolean async){
    if(layout.hasSearchableColumn())
      return buildOnDemandIndexCreationQuery(schema, table, index.extractAlias(), NEW_LAYOUT_INDEX_COULMN, async);
    return buildOnDemandIndexCreationQuery(schema, table, index.extractLogicalPropertyPath(), OLD_LAYOUT_INDEX_COULMN, async);
  }

  public static SQLQuery buildOnDemandIndexCreationQuery(String schema, String table, String propertyPath, String targetColumn, boolean async){
    return new SQLQuery((async ? "PERFORM " : "SELECT ") +
            """
            xyz_index_creation_on_property_object(
                #{schema_name},
                #{table_name},
                #{property_name},
                xyz_index_name_for_property(#{table_name}, #{property_name}, #{idx_type}),
                xyz_property_datatype(#{schema_name}, #{table_name}, #{property_name}, #{table_sample_cnt}, #{target_column} ),
                #{idx_type},
                #{target_column}
              )
            """)
            .withNamedParameter("schema_name", schema)
            .withNamedParameter("table_name", table)
            .withNamedParameter("property_name", propertyPath)
            .withNamedParameter("table_sample_cnt", 5000)
            .withNamedParameter("idx_type", "m")
            .withNamedParameter("target_column", targetColumn);
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
    if(layout.isOld())
      return Arrays.asList(SystemIndex.values()).stream()
              .map(index -> buildCreateIndexQuery(schema, table, index.getIndexContent(), index.getIndexType(), index.getIndexName(table)))
              .toList();
    //layout == NEW_LAYOUT

    return Stream.of(SystemIndex.GEO,
                    SystemIndex.NEXT_VERSION,
                    SystemIndex.VERSION_ID)
            .map(index -> buildCreateIndexQuery(
                    schema, table, index.getIndexContent(),
                    index.getIndexType(), index.getIndexName(table))
            ).toList();
  }
}
