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
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.ChangesetsStatisticsResponse;

import java.sql.ResultSet;
import java.sql.SQLException;

public class GetChangesetStatistics extends XyzQueryRunner<GetChangesetStatisticsEvent, ChangesetsStatisticsResponse> {
  Long minTagVersion;
  long minSpaceVersion;
  int versionsToKeep;

  public GetChangesetStatistics(GetChangesetStatisticsEvent event, DatabaseHandler dbHandler)
      throws SQLException, ErrorResponseException {
    super(event, dbHandler);
    setUseReadReplica(true);
    this.minSpaceVersion = event.getMinSpaceVersion();
    this.minTagVersion = event.getMinTagVersion();
    this.versionsToKeep = event.getVersionsToKeep();
  }

  @Override
  protected SQLQuery buildQuery(GetChangesetStatisticsEvent event) {
    SQLQuery query =new SQLQuery(
            "SELECT max(version) as max" +
                    "   from  ${schema}.${table} ");

    query.setVariable(SCHEMA, getSchema());
    query.setVariable(TABLE, getDefaultTable(event));

    return query;
  }

  @Override
  public ChangesetsStatisticsResponse handle(ResultSet rs) throws SQLException {
    ChangesetsStatisticsResponse csr = new ChangesetsStatisticsResponse();
    if(rs.next()){
      String mV = rs.getString("max");
      long maxVersion = (mV == null ? -1 : Long.parseLong(mV));
      csr.setMaxVersion(maxVersion);
      csr.setMinVersion(mV == null ? -1 : calculateMinVersion(maxVersion));
    }
    return csr;
  }

  private long calculateMinVersion(long maxVersion){
    if(minTagVersion != null){
      /** versionsToKeep doesn't matter. minSpaceVersion is always smaller than minTagVersion */
      return minSpaceVersion;
    }
    return Math.max(minSpaceVersion, maxVersion-versionsToKeep);
  }
}
