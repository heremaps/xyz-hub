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

import com.here.xyz.util.db.SQLQuery;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IndexHelper {

  public static SQLQuery buildCreateIndexQuery(String schema, String table, String columnName, String method) {
    return buildCreateIndexQuery(schema, table, Collections.singletonList(columnName), method);
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

  //TODO: move xyz_index_creation_on_property_object, xyz_index_name_for_property, xyz_property_datatype functions from
  // ext to common.sql ?
  public static SQLQuery buildAsyncOnDemandIndexQuery(String schema, String table, String propertyName){
    return new SQLQuery("""
            PERFORM xyz_index_creation_on_property_object(
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
            .withNamedParameter("property_name", propertyName)
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
}
