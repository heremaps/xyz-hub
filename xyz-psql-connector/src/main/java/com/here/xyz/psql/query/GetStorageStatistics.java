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
import com.here.xyz.events.Event;
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.responses.StatisticsResponse.Value;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.StorageStatistics.SpaceByteSizes;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class GetStorageStatistics extends XyzQueryRunner<GetStorageStatisticsEvent, StorageStatistics> {

  private static final String TABLE_NAME = "table_name";
  private static final String TABLE_BYTES = "table_bytes";
  private static final String INDEX_BYTES = "index_bytes";
  private final String connectorId;
  private final List<String> remainingSpaceIds;
  private Map<String, String> tableName2SpaceId;

  public GetStorageStatistics(GetStorageStatisticsEvent event)
      throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(true);
    remainingSpaceIds = new LinkedList<>(event.getSpaceIds());
    connectorId = ConnectorParameters.fromEvent(event).getConnectorId();
  }

  @Override
  protected SQLQuery buildQuery(GetStorageStatisticsEvent event) {
    List<String> tableNames = new ArrayList<>(event.getSpaceIds().size() * 2);
    event.getSpaceIds().forEach(spaceId -> {
      String tableName = resolveTableName(event, spaceId);
      tableNames.add(tableName);
    });

    return new SQLQuery("""
        WITH roots AS (
          SELECT to_regclass(t) as root_oid
          FROM unnest(ARRAY[${{tableNames}}]) AS t
          WHERE to_regclass(t) IS NOT NULL
        ),
        parts AS (
          SELECT DISTINCT relid
          FROM roots r,
               LATERAL pg_partition_tree(r.root_oid)
          WHERE level > 0
        ),
        sizes AS (
          SELECT
            oid,
            pg_total_relation_size(oid) AS total_bytes,
            pg_indexes_size(oid) AS index_bytes
          FROM pg_class
          WHERE oid IN (SELECT relid FROM parts)
        )
        SELECT
          c.relname AS table_name,
          s.total_bytes - s.index_bytes AS table_bytes,
          s.index_bytes
        FROM pg_class c
        JOIN sizes s ON s.oid = c.oid;
        """)
        .withQueryFragment("tableNames", tableNames
            .stream()
            .map(tableName -> "'\"" + getSchema() + "\".\"" + tableName + "\"'")
            .collect(Collectors.joining(",")))
        .withTimeout(15);
  }

  private String resolveTableName(Event event, String spaceId) {
    if (tableName2SpaceId == null)
      tableName2SpaceId = new HashMap<>();
    String tableName = XyzEventBasedQueryRunner.getTableNameForSpaceId(event, spaceId);
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
      boolean isHistoryTable = isHistoryTable(tableName);
      tableName = tableName.substring(0, tableName.lastIndexOf('_'));
      String spaceId = tableName2SpaceId.containsKey(tableName) ? tableName2SpaceId.get(tableName) : tableName;

      long tableBytes = rs.getLong(TABLE_BYTES),
           indexBytes = rs.getLong(INDEX_BYTES);

      SpaceByteSizes sizes = byteSizes.computeIfAbsent(spaceId, k -> new SpaceByteSizes().withStorageId(connectorId));
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

  private static boolean isHistoryTable(String tableName) {
    int suffixPos = tableName.lastIndexOf('_');
    if (suffixPos == -1)
      return false;
    String suffix = tableName.substring(suffixPos);
    if (suffix.startsWith("_p")) //The table is a history partition
      return true;
    return false;
  }
}
