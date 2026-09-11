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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.here.xyz.events.GetStorageStatisticsEvent;
import com.here.xyz.responses.StorageStatistics;
import com.here.xyz.util.Hasher;
import com.here.xyz.util.db.SQLQuery;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.RowSetMetaDataImpl;
import javax.sql.rowset.RowSetProvider;
import org.junit.jupiter.api.Test;

class GetStorageStatisticsTest {

  @Test
  void usesTheExplicitPhysicalTableName() {
    GetStorageStatisticsEvent event = new GetStorageStatisticsEvent()
        .withSpaceIdToTableName(Map.of("space-id", "physical_table"))
        .withConnectorParams(Map.of("enableHashedSpaceId", true));

    assertEquals("physical_table", GetStorageStatistics.resolvePhysicalTableName(event, "space-id"));
  }

  @Test
  void hashesLegacySpaceIdsWhenConfigured() {
    GetStorageStatisticsEvent event = new GetStorageStatisticsEvent()
        .withConnectorParams(Map.of("enableHashedSpaceId", true));

    assertEquals(Hasher.getHash("legacy-space"),
        GetStorageStatistics.resolvePhysicalTableName(event, "legacy-space"));
  }

  @Test
  void keepsLegacySpaceIdsWhenHashingIsDisabled() {
    GetStorageStatisticsEvent event = new GetStorageStatisticsEvent();

    assertEquals("legacy-space", GetStorageStatistics.resolvePhysicalTableName(event, "legacy-space"));
  }

  @Test
  void bindsPhysicalTableNamesAsQueryParameters() throws Exception {
    GetStorageStatisticsEvent event = event(List.of("space-id"),
        Map.of("space-id", "physical'table"));
    SQLQuery query = queryRunner(event).buildQuery(event);

    assertFalse(query.text().contains("physical'table"));
    assertEquals("\"public\".\"physical'table\"", query.getNamedParameters().get("tableName0"));
    assertFalse(query.substitute().text().contains("physical'table"));
  }

  @Test
  void reportsSharedPhysicalTablesForEveryLogicalSpaceId() throws Exception {
    GetStorageStatisticsEvent event = event(List.of("first-space", "second-space"), Map.of(
        "first-space", "shared_table",
        "second-space", "shared_table"));
    GetStorageStatistics queryRunner = queryRunner(event);
    queryRunner.buildQuery(event);

    StorageStatistics statistics = queryRunner.handle(resultSet("shared_table_head", 100, 25));

    assertEquals(100L, statistics.getByteSizes().get("first-space").getContentBytes().getValue());
    assertEquals(100L, statistics.getByteSizes().get("second-space").getContentBytes().getValue());
    assertEquals(25L, statistics.getByteSizes().get("first-space").getSearchablePropertiesBytes().getValue());
    assertEquals(25L, statistics.getByteSizes().get("second-space").getSearchablePropertiesBytes().getValue());
  }

  private static GetStorageStatisticsEvent event(List<String> spaceIds, Map<String, String> tableNames) {
    return new GetStorageStatisticsEvent()
        .withSpaceIds(spaceIds)
        .withSpaceIdToTableName(tableNames)
        .withConnectorParams(Map.of("connectorId", "test-connector"));
  }

  private static GetStorageStatistics queryRunner(GetStorageStatisticsEvent event) throws Exception {
    return new GetStorageStatistics(event) {
      @Override
      protected String getSchema() {
        return "public";
      }
    };
  }

  private static CachedRowSet resultSet(String tableName, long tableBytes, long indexBytes) throws Exception {
    RowSetMetaDataImpl metadata = new RowSetMetaDataImpl();
    metadata.setColumnCount(3);
    metadata.setColumnName(1, "table_name");
    metadata.setColumnType(1, Types.VARCHAR);
    metadata.setColumnName(2, "table_bytes");
    metadata.setColumnType(2, Types.BIGINT);
    metadata.setColumnName(3, "index_bytes");
    metadata.setColumnType(3, Types.BIGINT);

    CachedRowSet rowSet = RowSetProvider.newFactory().createCachedRowSet();
    rowSet.setMetaData(metadata);
    rowSet.moveToInsertRow();
    rowSet.updateString(1, tableName);
    rowSet.updateLong(2, tableBytes);
    rowSet.updateLong(3, indexBytes);
    rowSet.insertRow();
    rowSet.moveToCurrentRow();
    rowSet.beforeFirst();
    return rowSet;
  }
}