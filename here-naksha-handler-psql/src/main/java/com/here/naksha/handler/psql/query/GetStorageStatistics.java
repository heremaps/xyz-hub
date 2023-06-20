/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.naksha.handler.psql.query;

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;

import com.here.naksha.lib.psql.PsqlCollection;
import com.here.naksha.lib.core.models.payload.events.info.GetStorageStatisticsEvent;
import com.here.naksha.lib.core.models.payload.responses.StatisticsResponse.Value;
import com.here.naksha.lib.core.models.payload.responses.StorageStatistics;
import com.here.naksha.lib.core.models.payload.responses.StorageStatistics.SpaceByteSizes;
import com.here.naksha.handler.psql.PsqlHandler;
import com.here.naksha.handler.psql.SQLQueryExt;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class GetStorageStatistics extends XyzQueryRunner<GetStorageStatisticsEvent, StorageStatistics> {

    private static final String TABLE_NAME = "table_name";
    private static final String TABLE_BYTES = "table_bytes";
    private static final String INDEX_BYTES = "index_bytes";

    private final List<String> remainingSpaceIds;
    private Map<String, String> tableName2SpaceId;

    public GetStorageStatistics(@NotNull GetStorageStatisticsEvent event, @NotNull PsqlHandler psqlConnector)
            throws SQLException {
        super(event, psqlConnector);
        setUseReadReplica(true);
        remainingSpaceIds = new LinkedList<>(event.getSpaceIds());
    }

    @Override
    protected @NotNull SQLQueryExt buildQuery(@Nonnull GetStorageStatisticsEvent event) {
        final List<@NotNull String> tableNames =
                new ArrayList<>(event.getSpaceIds().size() * 2);
        final String schema = processor.spaceSchema();
        event.getSpaceIds().forEach(spaceId -> {
            final PsqlCollection space = processor.getSpaceById(spaceId);
            if (space == null) {
                currentLogger().info("Unknown space: {}", spaceId);
            } else if (!schema.equals(space.schema)) {
                currentLogger()
                        .error(
                                "The given space '{}' is located in schema '{}', but this connector is bound to schema '{}', ignore space",
                                spaceId,
                                space.schema,
                                schema);
            } else {
                tableNames.add(space.table);
                tableNames.add(space.table + "_hst");
            }
        });
        return new SQLQueryExt("SELECT relname                                                AS "
                + TABLE_NAME
                + ","
                + "       pg_indexes_size(c.oid)                                 AS "
                + INDEX_BYTES
                + ","
                + "       pg_total_relation_size(c.oid) - pg_indexes_size(c.oid) AS "
                + TABLE_BYTES
                + " FROM pg_class c"
                + "         LEFT JOIN pg_namespace n ON n.oid = c.relnamespace "
                + " WHERE relkind = 'r'"
                + " AND nspname = '"
                + schema
                + "'"
                + " AND relname IN ("
                + tableNames.stream().map(tableName -> "'" + tableName + "'").collect(Collectors.joining(","))
                + ")");
    }

    @Override
    public @NotNull StorageStatistics handle(@NotNull ResultSet rs) throws SQLException {
        final Map<@NotNull String, SpaceByteSizes> byteSizes = new HashMap<>();
        StorageStatistics stats = new StorageStatistics()
                .withCreatedAt(System.currentTimeMillis())
                .withByteSizes(byteSizes);

        // Read the space / history info from the returned ResultSet
        while (rs.next()) {
            String tableName = rs.getString(TABLE_NAME);
            boolean isHistoryTable = tableName.endsWith(PsqlHandler.HISTORY_TABLE_SUFFIX);
            tableName = isHistoryTable
                    ? tableName.substring(0, tableName.length() - PsqlHandler.HISTORY_TABLE_SUFFIX.length())
                    : tableName;
            String spaceId = tableName2SpaceId.containsKey(tableName) ? tableName2SpaceId.get(tableName) : tableName;

            long tableBytes = rs.getLong(TABLE_BYTES), indexBytes = rs.getLong(INDEX_BYTES);

            SpaceByteSizes sizes = byteSizes.computeIfAbsent(spaceId, k -> new SpaceByteSizes());
            if (isHistoryTable) {
                sizes.setHistoryBytes(new Value<>(tableBytes + indexBytes).withEstimated(true));
            } else {
                sizes.setContentBytes(new Value<>(tableBytes).withEstimated(true));
                sizes.setSearchablePropertiesBytes(new Value<>(indexBytes).withEstimated(true));
                remainingSpaceIds.remove(spaceId);
            }
        }

        // For non-existing tables (no features written to the space yet), set all values to zero
        remainingSpaceIds.forEach(spaceId -> byteSizes
                .computeIfAbsent(spaceId, k -> new SpaceByteSizes())
                .withContentBytes(new Value<>(0L).withEstimated(true)));

        return stats;
    }
}
