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
}
