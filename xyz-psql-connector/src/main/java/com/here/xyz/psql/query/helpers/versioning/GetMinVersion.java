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

package com.here.xyz.psql.query.helpers.versioning;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import com.here.xyz.util.db.SQLQuery;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GetMinVersion<E extends Event> extends XyzEventBasedQueryRunner<E, Long> {

  public GetMinVersion(E input) throws SQLException, ErrorResponseException {
    super(input);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT min(version) FROM ${schema}.${table}")
            .withVariable(SCHEMA, getSchema())
            .withVariable(TABLE, getDefaultTable(event));
  }

  @Override
  public Long handle(ResultSet rs) throws SQLException {
    if (rs.next()){
      return rs.getLong("min");
    }
    throw new SQLException("Unable to retrieve minVersion from space.");
  }
}
