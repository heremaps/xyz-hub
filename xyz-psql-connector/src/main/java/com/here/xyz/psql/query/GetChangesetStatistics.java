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

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.SCHEMA;
import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

import com.here.xyz.connectors.ErrorResponseException;
import com.here.xyz.events.GetChangesetStatisticsEvent;
import com.here.xyz.responses.ChangesetsStatisticsResponse;
import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.pg.XyzSpaceTableHelper;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GetChangesetStatistics extends XyzQueryRunner<GetChangesetStatisticsEvent, ChangesetsStatisticsResponse> {
  long minTagVersion;
  long minVersion;

  public GetChangesetStatistics(GetChangesetStatisticsEvent event)
      throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(true);

    this.minVersion = event.getMinVersion();
    this.minTagVersion = event.getMinTagVersion() != null ? event.getMinTagVersion() : -1L;
  }

  @Override
  protected SQLQuery buildQuery(GetChangesetStatisticsEvent event) {
    SQLQuery query =new SQLQuery(
            "SELECT (SELECT max(version) FROM ${schema}.${table}) as max," +
                    " (SELECT min(version) FROM ${schema}.${table}) as min;");

    query.setVariable(SCHEMA, getSchema());
    query.setVariable(TABLE, getDefaultTable(event));
    query.setNamedParameter(TABLE, getDefaultTable(event));

    return query;
  }

  @Override
  public ChangesetsStatisticsResponse handle(ResultSet rs) throws SQLException {
    ChangesetsStatisticsResponse csr = new ChangesetsStatisticsResponse();
    if(rs.next()){
      String maxV = rs.getString("max");
      String minV = rs.getString("min");
      long maxDbVersion = (maxV == null ? 0 : Long.parseLong(maxV));
      //FIXME: Returned minVersion is not always correct for spaces with v2k=1
      long minDbVersion = (minV == null ? 0 : Long.parseLong(minV));

      csr.setMaxVersion(maxDbVersion);
      //Default = 0
      if(minVersion != 0L)
        csr.setMinVersion(Math.min(minDbVersion, minVersion));
      else
        //We deliver 0 as minVersion to include the empty space version "0"
        csr.setMinVersion(minDbVersion == 1L ? 0L : minDbVersion);

      if(minTagVersion != -1L)
        csr.setTagMinVersion(minTagVersion);
    }
    return csr;
  }
}
