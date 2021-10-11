/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

import static com.here.xyz.psql.DatabaseHandler.HISTORY_TABLE_SUFFIX;

import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.psql.DatabaseHandler;
import com.here.xyz.psql.QueryRunner;
import com.here.xyz.psql.SQLQuery;
import com.here.xyz.responses.StatisticsResponse.Value;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.StorageStatistics.SpaceByteSizes;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StorageStatisticsQueryRunner extends QueryRunner<GetStorageStatisticsEvent, StorageStatistics> {

  private static final String TABLE_NAME = "table_name";
  private static final String TABLE_BYTES = "table_bytes";
  private static final String INDEX_BYTES = "index_bytes";

  private final List<String> remainingSpaceIds;
  private Map<String, String> tableName2SpaceId;

  public StorageStatisticsQueryRunner(GetStorageStatisticsEvent event, DatabaseHandler dbHandler) {
    super(event, dbHandler);
    setUseReadReplica(true);
    remainingSpaceIds = new LinkedList<>(event.getSpaceIds());
  }

  @Override
  protected SQLQuery buildQuery(GetStorageStatisticsEvent event) {
    List<String> tableNames = new ArrayList<>(event.getSpaceIds().size() * 2);
    event.getSpaceIds().forEach(spaceId -> {
      String tableName = resolveTableName(spaceId);
      tableNames.add(tableName);
      tableNames.add(tableName + HISTORY_TABLE_SUFFIX);
    });
    return new SQLQuery( "SELECT relname                                                AS " + TABLE_NAME + ","
                            + "       pg_indexes_size(c.oid)                                 AS " + INDEX_BYTES + ","
                            + "       pg_total_relation_size(c.oid) - pg_indexes_size(c.oid) AS " + TABLE_BYTES
                            + " FROM pg_class c"
                            + "         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace "
                            + " WHERE relkind = 'r'"
                            + " AND nspname = '" + getSchema() + "'"
                            + " AND relname IN (" + tableNames
                                                      .stream()
                                                      .map(tableName -> "'" + tableName + "'")
                                                      .collect(Collectors.joining(",")) + ")");
  }

  private String resolveTableName(String spaceId) {
    if (tableName2SpaceId == null)
      tableName2SpaceId = new HashMap<>();
    String tableName = dbHandler.getConfig().getTableNameForSpaceId(spaceId);
    if (!spaceId.equals(tableName))
      tableName2SpaceId.put(tableName, spaceId);
    return tableName;
  }

  @Override
  public StorageStatistics handle(ResultSet rs) throws SQLException {
    Map<String, SpaceByteSizes> byteSizes = new HashMap<>();
    StorageStatistics stats = new StorageStatistics()
        .withCreatedAt(System.currentTimeMillis())
        .withByteSizes(byteSizes);

    //Read the space / history info from the returned ResultSet
    while (rs.next()) {
      String tableName = rs.getString(TABLE_NAME);
      boolean isHistoryTable = tableName.endsWith(HISTORY_TABLE_SUFFIX);
      tableName = isHistoryTable ? tableName.substring(0, tableName.length() - HISTORY_TABLE_SUFFIX.length()) : tableName;
      String spaceId = tableName2SpaceId.containsKey(tableName) ? tableName2SpaceId.get(tableName) : tableName;

      long tableBytes = rs.getLong(TABLE_BYTES),
           indexBytes = rs.getLong(INDEX_BYTES);

      SpaceByteSizes sizes = byteSizes.computeIfAbsent(spaceId, k -> new SpaceByteSizes());
      if (isHistoryTable)
        sizes.setHistoryBytes(new Value<>(tableBytes + indexBytes).withEstimated(true));
      else {
        sizes.setContentBytes(new Value<>(tableBytes).withEstimated(true));
        sizes.setSearchablePropertiesBytes(new Value<>(indexBytes).withEstimated(true));
        remainingSpaceIds.remove(spaceId);
      }
    }

    //For non-existing tables (no features written to the space yet), set all values to zero
    remainingSpaceIds.forEach(spaceId -> byteSizes.computeIfAbsent(spaceId, k -> new SpaceByteSizes())
          .withContentBytes(new Value<>(0L).withEstimated(true)));

    return stats;
  }
}
