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

import static com.here.xyz.psql.query.ModifySpace.SPACE_META_TABLE_FQN;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetMinAvailableVersion<E extends Event> extends XyzEventBasedQueryRunner<E, Long> {

  public GetMinAvailableVersion(E input) throws SQLException, ErrorResponseException {
    super(input);
  }

  @Override
  protected SQLQuery buildQuery(E event) throws SQLException, ErrorResponseException {
    return new SQLQuery("SELECT COALESCE((meta->'minAvailableVersion')::bigint, 0) as min," +
            "(SELECT max(version) FROM ${schema}.${table}) as max " +
            " FROM " + SPACE_META_TABLE_FQN +
            " WHERE id=#{spaceId}")
        .withVariable(SCHEMA, getSchema())
        .withVariable(TABLE, getDefaultTable(event))
        .withNamedParameter("spaceId", event.getSpace());
  }

  @Override
  public Long handle(ResultSet rs) throws SQLException {
    if (rs.next()){
      return rs.getLong("min");
    }
    throw new SQLException("Unable to retrieve minAvailableVersion from space.");
  }
}
