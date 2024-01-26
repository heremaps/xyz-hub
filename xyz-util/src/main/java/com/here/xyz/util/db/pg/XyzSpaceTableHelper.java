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

import static com.here.xyz.util.db.pg.IndexHelper.buildCreateIndexQuery;

import com.here.xyz.util.db.SQLQuery;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class XyzSpaceTableHelper {

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

  public static List<SQLQuery> buildSpaceTableIndexQueries(String schema, String table) {
    return buildSpaceTableIndexQueries(schema, table, null);
  }

  private static SQLQuery addQueryComment(SQLQuery indexCreationQuery, SQLQuery queryComment) {
    return queryComment != null ? indexCreationQuery.withQueryFragment("queryComment", queryComment) : indexCreationQuery;
  }
}
