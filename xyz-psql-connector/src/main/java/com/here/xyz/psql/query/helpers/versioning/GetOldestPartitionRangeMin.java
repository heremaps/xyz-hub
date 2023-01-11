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

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.Event;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.psql.query.XyzEventBasedQueryRunner;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetOldestPartitionRangeMin extends XyzEventBasedQueryRunner<Event, Long> {

  private String tableName;

  public GetOldestPartitionRangeMin(Event input, DatabaseHandler dbHandler) throws SQLException, ErrorResponseException {
    super(input, dbHandler);
  }

  @Override
  protected SQLQuery buildQuery(Event event) throws SQLException, ErrorResponseException {
    tableName = getDefaultTable(event);
    return new SQLQuery("SELECT pg_get_expr(relpartbound, oid, true) FROM pg_class "
        + "WHERE relname LIKE #{partitionPattern} AND relkind = 'r'")
        .withNamedParameter("partitionPattern", tableName + "_p%");
  }

  private long getRangeMinFromExpression(String boundsExpression) {
    String boundsPart = boundsExpression.substring(18);
    return Long.parseLong(boundsPart.substring(0, boundsPart.indexOf("'")));
  }

  @Override
  public Long handle(ResultSet rs) throws SQLException {
    long minRangeMin = Long.MAX_VALUE;
    while (rs.next())
      minRangeMin = Math.min(minRangeMin, getRangeMinFromExpression(rs.getString(1)));
    if (minRangeMin == Long.MAX_VALUE)
      throw new SQLException("Unable to get oldest partition for table.");
    else
      return minRangeMin;
  }
}
