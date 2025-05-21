/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
import com.here.xyz.events.GetStatisticsEvent;
import com.here.xyz.responses.StatisticsResponse;
import com.here.xyz.responses.StatisticsResponse.Value;
import com.here.xyz.util.db.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.here.xyz.util.db.pg.XyzSpaceTableHelper.TABLE;

public class GetFastStatistics extends GetStatistics {

  public GetFastStatistics(GetStatisticsEvent event) throws SQLException, ErrorResponseException {
    super(event);
  }

  @Override
  protected SQLQuery buildQuery(GetStatisticsEvent event) throws SQLException, ErrorResponseException {
    return new SQLQuery("select table_size, table_count, is_estimated, min_version, max_version" +
            " FROM calculate_space_statistics(to_regclass(#{table}), to_regclass(#{extTable}), #{context});")
            .withNamedParameter(TABLE, getSchema() +".\""+ getDefaultTable(event) +"_head\"")
            .withNamedParameter("extTable", getSchema() +".\""+ getExtendedTable(event) + "_head\"")
            .withNamedParameter("context", event.getContext().name());
  }

  @Override
  public StatisticsResponse handle(ResultSet rs) throws SQLException {
    rs.next();

    Value<Long> tableSize =  new Value<>(rs.getLong("table_size")).withEstimated(true);
    Value<Long> count =  new Value<>(rs.getLong("table_count")).withEstimated(rs.getBoolean("is_estimated"));
    Value<Long> maxVersion =  new Value<>(rs.getLong("max_version")).withEstimated(false);
    Value<Long> minVersion =  new Value<>(rs.getLong("min_version")).withEstimated(false);

    return new StatisticsResponse()
            .withByteSize(tableSize)
            .withDataSize(tableSize)
            .withCount(count)
            .withMaxVersion(maxVersion)
            .withMinVersion(minVersion);
  }
}
