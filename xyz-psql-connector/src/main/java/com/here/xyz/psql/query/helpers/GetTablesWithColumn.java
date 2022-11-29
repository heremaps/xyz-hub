/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.psql.query.helpers;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.helpers.GetTablesWithColumn.GetTablesWithColumnInput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GetTablesWithColumn extends QueryRunner<GetTablesWithColumnInput, List<String>> {

  public GetTablesWithColumn(GetTablesWithColumnInput input, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(input, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(GetTablesWithColumnInput input) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT t.table_name FROM information_schema.tables t "
        + "INNER JOIN information_schema.columns c ON "
          + "c.table_name = t.table_name AND c.table_schema = t.table_schema "
        + "WHERE "
        + "c.column_name = #{column} AND "
        + "t.table_schema = #{schema} AND "
        + "t.table_type = 'BASE TABLE' AND "
        + "t.table_name != 'spatial_ref_sys' AND "
        + "t.table_name NOT LIKE '%_hst' "
        + (input.exists ? "" : "AND NOT exists(SELECT 1 FROM information_schema.columns WHERE table_name = t.table_name AND column_name = #{column}) ")
        + "LIMIT #{limit}")
        .withNamedParameter("column", input.columnName)
        .withNamedParameter("schema", getSchema())
        .withNamedParameter("limit", input.limit);
  }

  @Override
  public List<String> handle(ResultSet rs) throws SQLException {
    final ArrayList<String> result = new ArrayList<>();
    while (rs.next())
      result.add(rs.getString("table_name"));
    return result;
  }

  public static class GetTablesWithColumnInput {

    private String columnName;
    private boolean exists;
    private int limit;

    GetTablesWithColumnInput(String columnName, boolean exists) {
      this(columnName, exists, Integer.MAX_VALUE);
    }

    public GetTablesWithColumnInput(String columnName, boolean exists, int limit) {
      this.columnName = columnName;
      this.exists = exists;
      this.limit = limit;
    }
  }
}
