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
package com.here.naksha.lib.psql;

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteCollections;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.util.CloseableResource;
import com.here.naksha.lib.core.util.json.Json;
import com.vividsolutions.jts.geom.Coordinate;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.postgresql.jdbc.PgConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PostgresQL session being backed by the PostgresQL data connection. It keeps track of all open cursors as resource children and
 * guarantees that all cursors are closed before the underlying connection is closed.
 */
final class PostgresSession extends CloseableResource<PostgresStorage> {

  private static final Logger log = LoggerFactory.getLogger(PostgresSession.class);

  // There are two facades: PsqlReadSession and PsqlWriteSession.
  // They only differ in that they set the last parameter to true or false.
  PostgresSession(
      @NotNull PsqlSession proxy,
      @NotNull PostgresStorage storage,
      @NotNull NakshaContext context,
      @NotNull Connection connection,
      boolean readOnly) {
    super(proxy, storage);
    this.context = context;
    this.connection = connection;
    this.readOnly = readOnly;
    this.config = storage().dataSource.getConfig();
    this.sql = new SQL();
  }

  @NotNull
  PostgresStorage storage() {
    final PostgresStorage storage = super.parent();
    assert storage != null;
    return storage;
  }

  /**
   * The context to be used.
   */
  final @NotNull NakshaContext context;

  final @NotNull Connection connection;
  final boolean readOnly;
  final @NotNull PsqlStorageConfig config;
  private final @NotNull SQL sql;
  // Statement timeout in milliseconds.
  long stmtTimeout = -1;
  // Lock timeout in milliseconds.
  long lockTimeout = -1;
  // The amount of features to fetch at ones.
  int fetchSize = 1000;

  @Override
  protected void destruct() {
    try {
      connection.rollback();
    } catch (SQLException e) {
      log.atInfo()
          .setMessage("Error while trying to rollback connection")
          .setCause(e)
          .log();
    }
    try {
      connection.close();
    } catch (Exception e) {
      log.atInfo()
          .setMessage("Failed to close PostgresQL connection")
          .setCause(e)
          .log();
    }
  }

  @NotNull
  SQL sql() {
    sql.setLength(0);
    if (stmtTimeout >= 0) {
      sql.add("SET LOCAL statement_timeout TO ").add(stmtTimeout).add(";\n");
    }
    if (lockTimeout >= 0) {
      sql.add("SET LOCAL lock_timeout TO ").add(lockTimeout).add(";\n");
    }
    return sql;
  }

  @SuppressWarnings("SqlSourceToSinkFlow")
  @NotNull
  PreparedStatement prepareWithCursor(@NotNull CharSequence query) {
    try {
      PreparedStatement stmt = connection.prepareStatement(
          query.toString(),
          ResultSet.TYPE_SCROLL_INSENSITIVE,
          ResultSet.CONCUR_READ_ONLY,
          ResultSet.HOLD_CURSORS_OVER_COMMIT);
      stmt.setFetchSize(fetchSize);
      return stmt;
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  void commit(boolean autoCloseCursors) throws SQLException {

  }

  void rollback(boolean autoCloseCursors) throws SQLException {

  }

  void close(boolean autoCloseCursors) {

  }

  @SuppressWarnings("SqlSourceToSinkFlow")
  @NotNull
  PreparedStatement prepareWithoutCursor(@NotNull CharSequence query) {
    try {
      PreparedStatement stmt = connection.prepareStatement(
          query.toString(),
          ResultSet.TYPE_FORWARD_ONLY,
          ResultSet.CONCUR_READ_ONLY,
          ResultSet.CLOSE_CURSORS_AT_COMMIT);
      stmt.setFetchSize(fetchSize);
      return stmt;
    } catch (SQLException e) {
      throw unchecked(e);
    }
  }

  private static void assure3d(@NotNull Coordinate @NotNull [] coords) {
    for (final @NotNull Coordinate coord : coords) {
      if (coord.z != coord.z) { // if coord.z is NaN
        coord.z = 0;
      }
    }
  }

  @NotNull
  PgConnection pgConnection() {
    return (PgConnection) connection;
  }

  @NotNull
  Result process(@NotNull Notification<?> notification) {
    return new ErrorResult(XyzError.NOT_IMPLEMENTED, "process");
  }

  @NotNull
  Result executeRead(@NotNull ReadRequest<?> readRequest) {
    if (readRequest instanceof final ReadFeatures readFeatures) {
      final List<@NotNull String> collections = readFeatures.getCollections();
      // TODO read multiple collections
      final PreparedStatement stmt = prepare(sql().add(
              "SELECT jsondata->'properties'->'@ns:com:here:xyz'->>'action', jsondata->>'id', jsondata->'properties'->'@ns:com:here:xyz'->>'uuid', jsondata->>'type', 'TODO' as r_ptype, jsondata::jsonb, ST_AsEWKB(geo)")
          .add(" FROM ")
          .addIdent(collections.get(0))
          .add("  WHERE jsondata->>'id' = ?;"));
      try {
        // TODO create dynamic where section
        stmt.setString(1, readFeatures.getPropertyOp().value().toString());
        return new PsqlSuccess(new PsqlCursor<>(this, stmt, stmt.executeQuery()));
      } catch (SQLException e) {
        try {
          stmt.close();
        } catch (Throwable ce) {
          log.info("Failed to close statement", ce);
        }
        throw unchecked(e);
      }
    }
    return new ErrorResult(XyzError.NOT_IMPLEMENTED, "executeRead");
  }

  @NotNull
  <T> Result executeWrite(@NotNull WriteRequest<T, ?> writeRequest) {
    if (writeRequest instanceof WriteCollections<?>) {
      final PreparedStatement stmt = prepareWithCursor(
          sql().add(
                  "SELECT r_op, r_id, r_uuid, r_type, r_ptype, r_feature, r_geometry FROM naksha_write_collections(?);\n"));
      try (final Json json = Json.get()) {
        final @NotNull List<? extends WriteOp<T>> queries = writeRequest.features;
        final int SIZE = writeRequest.features.size();
        final String[] write_ops_json = new String[SIZE];
        final PostgresWriteOp out = new PostgresWriteOp();
        for (int i = 0; i < SIZE; i++) {
          convert(json, queries.get(i), out);
          write_ops_json[i] = json.writer().writeValueAsString(out);
        }
        stmt.setArray(1, connection.createArrayOf("jsonb", write_ops_json));
        return new PsqlSuccess(new PsqlCursor<>(this, stmt, stmt.executeQuery()));
      } catch (Throwable e) {
        try {
          stmt.close();
        } catch (Throwable ce) {
          log.info("Failed to close statement", ce);
        }
        throw unchecked(e);
      }
    }
    if (writeRequest instanceof final WriteFeatures<?> writeFeatures) {
      final int partition_id;
      //noinspection rawtypes
      if (writeFeatures instanceof PostgresWriteFeaturesToPartition writeToPartition) {
        partition_id = writeToPartition.partitionId;
      } else {
        partition_id = -1;
      }
      final PreparedStatement stmt = prepareWithCursor(
          "SELECT r_op, r_id, r_uuid, r_type, r_ptype, r_feature, ST_AsEWKB(r_geometry) FROM nk_write_features(?,?,?,?,?);");
      try (final Json json = Json.get()) {
        final @NotNull List<? extends WriteOp<T>> queries = writeRequest.features;
        final int SIZE = writeRequest.features.size();
        final String[] write_ops_json = new String[SIZE];
        final byte[][] geometries = new byte[SIZE][];
        final PostgresWriteOp out = new PostgresWriteOp();
        for (int i = 0; i < SIZE; i++) {
          convert(json, queries.get(i), out);
          write_ops_json[i] = json.writer().writeValueAsString(out);
          geometries[i] = out.geometry;
        }
        stmt.setString(1, writeFeatures.getCollectionId());
        stmt.setInt(2, partition_id);
        stmt.setArray(3, connection.createArrayOf("jsonb", write_ops_json));
        stmt.setArray(4, connection.createArrayOf("bytea", geometries));
        stmt.setBoolean(5, writeFeatures.minResults);
        return new PsqlSuccess(new PsqlCursor<>(this, stmt, stmt.executeQuery()));
      } catch (Throwable e) {
        try {
          stmt.close();
        } catch (Throwable ce) {
          log.info("Failed to close statement", ce);
        }
        throw unchecked(e);
      }
    }
    return new ErrorResult(XyzError.NOT_IMPLEMENTED, "The supplied write-request is not yet implemented");
  }

  @NotNull
  IStorageLock lockFeature(
      @NotNull String collectionId, @NotNull String featureId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    throw new StorageLockException("Unsupported operation");
  }

  @NotNull
  IStorageLock lockStorage(@NotNull String lockId, long timeout, @NotNull TimeUnit timeUnit)
      throws StorageLockException {
    throw new StorageLockException("Unsupported operation");
  }
}
