/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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
import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.responses.StatisticsResponse.Value;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.responses.StorageStatistics.SpaceByteSizes;
import com.here.xyz.util.db.ConnectorParameters;
import com.here.xyz.util.db.SQLQuery;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class GetStorageStatistics extends XyzQueryRunner<GetStorageStatisticsEvent, StorageStatistics> {

  private static final String TABLE_NAME = "table_name";
  private static final String TABLE_BYTES = "table_bytes";
  private static final String INDEX_BYTES = "index_bytes";
  private final String connectorId;
  private final Set<String> remainingSpaceIds;
  private Map<String, Set<String>> tableNameToSpaceIds;

  public GetStorageStatistics(GetStorageStatisticsEvent event)
      throws SQLException, ErrorResponseException {
    super(event);
    setUseReadReplica(true);
    remainingSpaceIds = new LinkedHashSet<>(event.getSpaceIds());
    connectorId = ConnectorParameters.fromEvent(event).getConnectorId();
  }

  @Override
  protected SQLQuery buildQuery(GetStorageStatisticsEvent event) {
    List<String> tableNames = event.getSpaceIds().stream()
        .map(spaceId -> resolveTableName(event, spaceId))
        .distinct()
        .toList();

    String tableNameParameters = IntStream.range(0, tableNames.size())
        .mapToObj(i -> "#{tableName" + i + "}")
        .collect(Collectors.joining(","));
    SQLQuery query = new SQLQuery("""
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
        .withQueryFragment("tableNames", tableNameParameters)
        .withTimeout(15);
    for (int i = 0; i < tableNames.size(); i++)
      query.withNamedParameter("tableName" + i, qualifiedTableName(tableNames.get(i)));
    return query;
  }

  private String resolveTableName(GetStorageStatisticsEvent event, String spaceId) {
    if (tableNameToSpaceIds == null)
      tableNameToSpaceIds = new HashMap<>();

    String tableName = resolvePhysicalTableName(event, spaceId);
    tableNameToSpaceIds.computeIfAbsent(tableName, k -> new LinkedHashSet<>()).add(spaceId);
    return tableName;
  }

  private String qualifiedTableName(String tableName) {
    return "\"" + getSchema().replace("\"", "\"\"") + "\".\"" + tableName.replace("\"", "\"\"") + "\"";
  }

  static String resolvePhysicalTableName(GetStorageStatisticsEvent event, String spaceId) {
    Map<String, String> explicitTableNames = event.getSpaceIdToTableName();
    String tableName = explicitTableNames != null ? explicitTableNames.get(spaceId) : null;
    if (tableName == null || tableName.isEmpty())
      tableName = XyzEventBasedQueryRunner.getTableNameForSpaceId(event, spaceId);
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
      Set<String> spaceIds = tableNameToSpaceIds.getOrDefault(tableName, Set.of(tableName));

      long tableBytes = rs.getLong(TABLE_BYTES),
           indexBytes = rs.getLong(INDEX_BYTES);

      for (String spaceId : spaceIds) {
        SpaceByteSizes sizes = byteSizes.computeIfAbsent(spaceId, k -> new SpaceByteSizes().withStorageId(connectorId));
        if (isHistoryTable)
          sizes.setHistoryBytes(new Value<>(tableBytes + indexBytes).withEstimated(true));
        else {
          sizes.setContentBytes(new Value<>(tableBytes).withEstimated(true));
          sizes.setSearchablePropertiesBytes(new Value<>(indexBytes).withEstimated(true));
          remainingSpaceIds.remove(spaceId);
        }
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
