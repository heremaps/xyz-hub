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

package com.here.xyz.psql.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Supplier;
import org.apache.commons.dbutils.ResultSetHandler;

public class InlineQueryRunner<R> extends QueryRunner<Void, R> {

  private final Supplier<SQLQuery> querySupplier;
  private final ResultSetHandler<R> handler;

  public InlineQueryRunner(Supplier<SQLQuery> querySupplier) throws SQLException, ErrorResponseException {
    this(querySupplier, null);
  }

  public InlineQueryRunner(Supplier<SQLQuery> querySupplier, ResultSetHandler<R> handler) throws SQLException, ErrorResponseException {
    super(null);
    this.querySupplier = querySupplier;
    this.handler = handler;
  }

  @Override
  protected SQLQuery buildQuery(Void input) throws SQLException, ErrorResponseException {
    return querySupplier.get();
  }

  @Override
  public R handle(ResultSet rs) throws SQLException {
    if (handler == null)
      return null;
    return handler.handle(rs);
  }
}
