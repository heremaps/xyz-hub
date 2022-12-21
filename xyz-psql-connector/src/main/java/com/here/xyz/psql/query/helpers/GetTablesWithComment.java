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
import com.here.xyz.psql.query.helpers.GetTablesWithComment.GetTablesWithCommentInput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class GetTablesWithComment extends QueryRunner<GetTablesWithCommentInput, List<String>> {

  public GetTablesWithComment(GetTablesWithCommentInput input, DatabaseHandler dbHandler)
      throws SQLException, ErrorResponseException {
    super(input, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(GetTablesWithCommentInput input) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT t.table_name "
        + "FROM information_schema.tables t "
        + "         INNER JOIN pg_catalog.pg_class pgc "
        + "         ON t.table_name = pgc.relname "
        + "WHERE "
        + "  t.table_name != 'spatial_ref_sys' "
        + "  AND t.table_name NOT LIKE '%_hst' "
        + "  AND t.table_name NOT LIKE '%_head' "
        + "  AND t.table_name NOT LIKE '%_p%' "
        + "  AND t.table_type = 'BASE TABLE' "
        + "  AND t.table_schema = 'public' "
        + "  AND reltuples < #{tableSizeLimit} "
        + "  AND (${{comment}} ${{commentComparison}}) "
        + "LIMIT #{limit}")
        .withQueryFragment("comment", "pg_catalog.obj_description(pgc.oid, 'pg_class')")
        .withQueryFragment("commentComparison",
            input.comment == null ? "IS " + (input.exists ? "" : "NOT ") + "NULL" : (input.exists ? "= #{comment}" : "IS NULL OR ${{comment}} != #{comment}"))
        .withNamedParameter("limit", input.limit)
        .withNamedParameter("comment", input.comment)
        .withNamedParameter("tableSizeLimit", input.tableSizeLimit);
  }

  @Override
  public List<String> handle(ResultSet rs) throws SQLException {
    final ArrayList<String> result = new ArrayList<>();
    while (rs.next())
      result.add(rs.getString("table_name"));
    return result;
  }

  public static class GetTablesWithCommentInput {

    private String comment;
    private boolean exists;

    private int tableSizeLimit;
    private int limit;

    public GetTablesWithCommentInput(String comment, boolean exists, int tableSizeLimit, int limit) {
      this.comment = comment;
      this.exists = exists;
      this.tableSizeLimit = tableSizeLimit;
      this.limit = limit;
    }
  }
}
