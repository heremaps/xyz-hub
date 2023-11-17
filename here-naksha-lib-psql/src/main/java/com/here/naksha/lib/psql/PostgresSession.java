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
import static com.here.naksha.lib.core.models.storage.XyzCodecFactory.getFactory;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.FeatureCodec;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteCollections;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodecFactory;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.util.ClosableChildResource;
import com.here.naksha.lib.core.util.json.Json;
import com.vividsolutions.jts.geom.Coordinate;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PostgresQL session being backed by the PostgresQL connection. It keeps track of all open cursors as resource children and guarantees
 * that all cursors are closed before the underlying connection is closed.
 */
final class PostgresSession extends ClosableChildResource<PostgresStorage> {

  private static final Logger log = LoggerFactory.getLogger(PostgresSession.class);

  // There are two facades: PsqlReadSession and PsqlWriteSession.
  // They only differ in that they set the last parameter to true or false.
  PostgresSession(
      @NotNull PsqlSession proxy,
      @NotNull PostgresStorage storage,
      @NotNull NakshaContext context,
      @NotNull PsqlConnection psqlConnection) {
    super(proxy, storage);
    this.context = context;
    this.psqlConnection = psqlConnection;
    this.readOnly = psqlConnection.connection.parent().config.readOnly;
    this.sql = new SQL();
    this.fetchSize = storage.getFetchSize();
    this.stmtTimeoutMillis = storage.getLockTimeout(MILLISECONDS);
    this.lockTimeoutMillis = storage.getLockTimeout(MILLISECONDS);
  }

  /**
   * The context to be used.
   */
  final @NotNull NakshaContext context;

  int fetchSize;
  long stmtTimeoutMillis;
  long lockTimeoutMillis;

  final @NotNull PsqlConnection psqlConnection;
  final boolean readOnly;
  private final @NotNull SQL sql;

  @Override
  protected void destruct() {
    try {
      psqlConnection.close();
    } catch (Exception e) {
      log.atInfo()
          .setMessage("Failed to close PostgresQL connection")
          .setCause(e)
          .log();
    }
  }

  int getFetchSize() {
    return fetchSize;
  }

  void setFetchSize(int size) {
    if (size <= 1) {
      throw new IllegalArgumentException("The fetchSize must be greater than zero");
    }
    this.fetchSize = size;
  }

  long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(stmtTimeoutMillis, MILLISECONDS);
  }

  void setStatementTimeout(long timeout, @NotNull TimeUnit timeUnit) throws SQLException {
    if (timeout < 0) {
      throw new IllegalArgumentException("The timeout must be greater/equal zero");
    }
    final long stmtTimeoutMillis = MILLISECONDS.convert(timeout, timeUnit);
    if (stmtTimeoutMillis != this.stmtTimeoutMillis) {
      this.stmtTimeoutMillis = stmtTimeoutMillis;
      executeStatement(sql().add("SET SESSION statement_timeout TO ")
          .add(stmtTimeoutMillis)
          .add(";\n"));
    }
  }

  long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(lockTimeoutMillis, MILLISECONDS);
  }

  void setLockTimeout(long timeout, @NotNull TimeUnit timeUnit) throws SQLException {
    if (timeout < 0) {
      throw new IllegalArgumentException("The timeout must be greater/equal zero");
    }
    final long lockTimeoutMillis = MILLISECONDS.convert(timeout, timeUnit);
    if (this.lockTimeoutMillis != lockTimeoutMillis) {
      this.lockTimeoutMillis = lockTimeoutMillis;
      executeStatement(sql().add("SET SESSION lock_timeout TO ")
          .add(lockTimeoutMillis)
          .add(";\n"));
    }
  }

  @NotNull
  SQL sql() {
    sql.setLength(0);
    return sql;
  }

  void executeStatement(@NotNull CharSequence query) throws SQLException {
    try (final Statement stmt = psqlConnection.createStatement()) {
      stmt.execute(query.toString());
    }
  }

  @SuppressWarnings("SqlSourceToSinkFlow")
  @NotNull
  PreparedStatement prepareStatement(@NotNull CharSequence query) {
    try {
      final PreparedStatement stmt = psqlConnection.prepareStatement(
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

  void commit(boolean autoCloseCursors) throws SQLException {
    // TODO: Apply autoCloseCursors
    psqlConnection.commit();
  }

  void rollback(boolean autoCloseCursors) throws SQLException {
    // TODO: Apply autoCloseCursors
    psqlConnection.rollback();
  }

  void close(boolean autoCloseCursors) {
    // TODO: Apply autoCloseCursors
    psqlConnection.close();
  }

  private static void assure3d(@NotNull Coordinate @NotNull [] coords) {
    for (final @NotNull Coordinate coord : coords) {
      if (coord.z != coord.z) { // if coord.z is NaN
        coord.z = 0;
      }
    }
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
  <FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>> Result executeWrite(
      @NotNull WriteRequest<FEATURE, CODEC, ?> writeRequest) {
    if (writeRequest instanceof WriteCollections) {
      final PreparedStatement stmt = prepareStatement(
          "SELECT r_op, r_id, r_uuid, r_type, r_ptype, r_feature, r_geometry FROM naksha_write_collections(?);\n");
      try (final Json json = Json.get()) {
        final List<@NotNull CODEC> features = writeRequest.features;
        final int SIZE = writeRequest.features.size();
        final String[] write_ops_json = new String[SIZE];
        final PostgresWriteOp out = new PostgresWriteOp();
        for (int i = 0; i < SIZE; i++) {
          final CODEC codec = features.get(i);
          out.decode(codec);
          write_ops_json[i] = json.writer().writeValueAsString(out);
        }
        stmt.setArray(1, psqlConnection.createArrayOf("jsonb", write_ops_json));
        return new PsqlSuccess(
            new PsqlCursor<>(getFactory(XyzCollectionCodecFactory.class), this, stmt, stmt.executeQuery()));
      } catch (Throwable e) {
        try {
          stmt.close();
        } catch (Throwable ce) {
          log.info("Failed to close statement", ce);
        }
        throw unchecked(e);
      }
    }
    if (writeRequest instanceof final WriteFeatures<?, ?, ?> writeFeatures) {
      final int partition_id = -1;
      //      if (writeFeatures instanceof PostgresWriteFeaturesToPartition<?> writeToPartition) {
      //        partition_id = writeToPartition.partitionId;
      //      } else {
      //        partition_id = -1;
      //      }
      final PreparedStatement stmt = prepareStatement(
          "SELECT r_op, r_id, r_uuid, r_type, r_ptype, r_feature, ST_AsEWKB(r_geometry) FROM nk_write_features(?,?,?,?,?);");
      try (final Json json = Json.get()) {
        final List<@NotNull CODEC> features = writeRequest.features;
        final int SIZE = writeRequest.features.size();
        final String[] write_ops_json = new String[SIZE];
        final byte[][] geometries = new byte[SIZE][];
        final PostgresWriteOp out = new PostgresWriteOp();
        for (int i = 0; i < SIZE; i++) {
          final CODEC codec = features.get(i);
          out.decode(codec);
          write_ops_json[i] = json.writer().writeValueAsString(out);
          geometries[i] = codec.getWkb();
        }
        stmt.setString(1, writeFeatures.getCollectionId());
        stmt.setInt(2, partition_id);
        stmt.setArray(3, psqlConnection.createArrayOf("jsonb", write_ops_json));
        stmt.setArray(4, psqlConnection.createArrayOf("bytea", geometries));
        stmt.setBoolean(5, writeFeatures.minResults);
        return new PsqlSuccess(
            new PsqlCursor<>(getFactory(XyzFeatureCodecFactory.class), this, stmt, stmt.executeQuery()));
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
