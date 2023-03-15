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
import com.here.xyz.psql.PsqlProcessor;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQueryExt;
import com.here.xyz.psql.query.helpers.FetchExistingIds.FetchIdsInput;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class FetchExistingIds extends QueryRunner<FetchIdsInput, List<String>> {

  public FetchExistingIds(@Nonnull FetchIdsInput input, @NotNull PsqlProcessor processor) throws SQLException, ErrorResponseException {
    super(input, processor);
  }

  @Override
  protected @NotNull SQLQueryExt buildQuery(@Nonnull FetchIdsInput input) throws SQLException, ErrorResponseException {
    SQLQueryExt query = new SQLQueryExt("SELECT jsondata->>'id' id FROM ${schema}.${table} WHERE jsondata->>'id' = ANY(#{ids})");
    query.setVariable(SCHEMA, processor.spaceSchema());
    query.setVariable(TABLE, input.targetTable);
    query.setNamedParameter("ids", input.idsToFetch.toArray(new String[0]));
    return query;
  }

  @Nonnull
  @Override
  public List<String> handle(@Nonnull ResultSet rs) throws SQLException {
    final ArrayList<String> result = new ArrayList<>();
    while (rs.next())
      result.add(rs.getString("id"));
    return result;
  }

  public static class FetchIdsInput {
    public FetchIdsInput(String targetTable, Collection<String> idsToFetch) {
      this.targetTable = targetTable;
      this.idsToFetch = idsToFetch;
    }
    String targetTable;
    Collection<String> idsToFetch;
  }
}
