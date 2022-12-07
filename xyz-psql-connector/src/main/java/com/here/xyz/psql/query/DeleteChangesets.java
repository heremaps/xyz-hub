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

package com.here.xyz.psql.query;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.ChangesetEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.SuccessResponse;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DeleteChangesets extends XyzQueryRunner<ChangesetEvent, SuccessResponse> {

  public DeleteChangesets(ChangesetEvent event, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(event, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(ChangesetEvent event) throws SQLException {
    return new SQLQuery("DELETE FROM ${schema}.${table} where version < #{version} and next_version <> #{max_version};")
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, getDefaultTable(event))
        .withNamedParameter("version", event.getHistoryVersion())
        .withNamedParameter("max_version", Long.MAX_VALUE);
  }

  @Override
  public SuccessResponse handle(ResultSet rs) throws SQLException {
    return new SuccessResponse();
  }
}
