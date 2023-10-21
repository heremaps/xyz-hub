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
package com.here.naksha.handler.psql;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.psql.PsqlConfig;
import com.here.naksha.lib.psql.PsqlDataSource;
import com.here.naksha.lib.psql.PsqlPool;
import com.here.naksha.lib.psql.PsqlPoolConfig;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import org.apache.commons.lang3.RandomUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The data-source used by the {@link PsqlHandler}. */
public class PsqlHandlerDataSource extends PsqlDataSource {

  private static @NotNull PsqlPool readOnlyPool(@NotNull PsqlHandlerParams params) {
    final List<@NotNull PsqlConfig> dbReplicas = params.getDbReplicas();
    final int SIZE = dbReplicas.size();
    if (SIZE == 0) {
      final PsqlPoolConfig dbConfig = params.getDbConfig();
      return PsqlPool.get(dbConfig);
    }
    final int replicaIndex = RandomUtils.nextInt(0, SIZE);
    final PsqlPoolConfig dbConfig = dbReplicas.get(replicaIndex);
    return PsqlPool.get(dbConfig);
  }

  private static @NotNull PsqlConfig readOnlyPsqlConfig(@NotNull PsqlHandlerParams params) {
    final List<@NotNull PsqlConfig> dbReplicas = params.getDbReplicas();
    final int SIZE = dbReplicas.size();
    if (SIZE == 0) {
      return params.getDbConfig();
    }
    final int replicaIndex = RandomUtils.nextInt(0, SIZE);
    return dbReplicas.get(replicaIndex);
  }

  /*@Override
  protected @NotNull String defaultSchema() {
  return "postgres";
  }*/

  /**
   * Create a new data source for the given connection pool and application.
   *
   * @param params the PostgresQL connector parameters.
   * @param spaceId the space identifier.
   * @param readOnly true if the connection should use a read-replica, if available; false
   *     otherwise.
   * @param collection the collection to use; if {@code null}, the space-id is used.
   */
  public PsqlHandlerDataSource(
      @NotNull PsqlHandlerParams params, @NotNull String spaceId, @Nullable String collection, boolean readOnly) {
    super(readOnly ? readOnlyPsqlConfig(params) : params.getDbConfig());
    this.readOnly = readOnly;
    this.connectorParams = params;
    // setSchema(params.getDbConfig().schema);
    // setRole(params.getDbConfig().role);
    this.spaceId = spaceId;
    this.table = collection != null ? collection : spaceId;
    this.historyTable = collection + "_hst";
    this.deletionTable = collection + "_del";
  }

  public final Connection openConnection(@NotNull String appId, String author) throws SQLException {
    final Connection conn = super.getConnection();
    try (final PreparedStatement stmt = conn.prepareStatement("SELECT naksha_tx_start(?, ?, ?);")) {
      stmt.setString(1, appId);
      stmt.setString(2, author);
      stmt.setBoolean(3, !readOnly);
      stmt.execute();
    } catch (final Exception e) {
      throw unchecked(e);
    }
    return conn;
  }

  /** The connector parameters used to create this data source. */
  public final @NotNull PsqlHandlerParams connectorParams;

  /** The space identifier. */
  public final @NotNull String spaceId;

  /** The database table, called “collection” in the event. */
  public final @NotNull String table;

  /** The name of the history table. */
  public final @NotNull String historyTable;

  /** The name of the deletion table. */
  public final @NotNull String deletionTable;

  /** True if this is a read-only source; false otherwise. */
  public final boolean readOnly;
}
