/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.helpers.TableExists.Table;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A helper QR that will test if the specified table exists.
 */
public class TableExists extends QueryRunner<Table, Boolean> {

  public TableExists(Table table) throws SQLException, ErrorResponseException {
    super(table);
  }

  @Override
  protected SQLQuery buildQuery(Table table) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT FROM pg_tables WHERE schemaname = #{schema} AND tablename = #{tableName}")
        .withNamedParameter("schema", table.schema)
        .withNamedParameter("tableName", table.tableName);
  }

  @Override
  public Boolean handle(ResultSet rs) throws SQLException {
    return rs.next();
  }

  public static class Table {
    private String schema;
    private String tableName;

    public Table(String schema, String tableName) {
      this.schema = schema;
      this.tableName = tableName;
    }
  }
}