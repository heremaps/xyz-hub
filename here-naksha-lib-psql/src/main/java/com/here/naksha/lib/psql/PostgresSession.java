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
import static java.util.Comparator.comparing;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.StorageLockException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.FeatureCodec;
import com.here.naksha.lib.core.models.storage.Notification;
import com.here.naksha.lib.core.models.storage.OpType;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.POpType;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SOp;
import com.here.naksha.lib.core.models.storage.SOpType;
import com.here.naksha.lib.core.models.storage.WriteCollections;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodecFactory;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.here.naksha.lib.core.storage.IStorageLock;
import com.here.naksha.lib.core.util.ClosableChildResource;
import com.here.naksha.lib.core.util.IndexHelper;
import com.here.naksha.lib.core.util.json.Json;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A PostgresQL session being backed by the PostgresQL connection. It keeps track of all open cursors as resource children and guarantees
 * that all cursors are closed before the underlying connection is closed.
 */
@SuppressWarnings("DuplicatedCode")
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
    this.readOnly = psqlConnection.postgresConnection.parent().config.readOnly;
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

  private static void addSpatialQuery(@NotNull SQL sql, @NotNull SOp spatialOp, @NotNull List<byte[]> wkbs) {
    final OpType op = spatialOp.op();
    if (SOpType.AND == op || SOpType.OR == op || SOpType.NOT == op) {
      final List<@NotNull SOp> children = spatialOp.children();
      if (children == null || children.size() == 0) {
        return;
      }
      final String op_literal;
      if (SOpType.AND == op) {
        op_literal = " AND";
      } else if (SOpType.OR == op) {
        op_literal = " OR";
      } else {
        op_literal = " NOT";
      }
      boolean first = true;
      sql.add('(');
      for (final @NotNull SOp child : children) {
        if (first) {
          first = false;
        } else {
          sql.add(op_literal);
        }
        addSpatialQuery(sql, child, wkbs);
      }
      sql.add(")");
    } else if (SOpType.INTERSECTS == op) {
      final Geometry geometry = spatialOp.getGeometry();
      if (geometry == null) {
        throw new IllegalArgumentException("Missing geometry");
      }
      sql.add(" ST_Intersects(geo, ST_Force3D(?))");
      try (final Json jp = Json.get()) {
        final byte[] wkb = jp.wkbWriter.write(geometry);
        wkbs.add(wkb);
      }
    } else {
      throw new IllegalArgumentException("Unknown operation: " + op);
    }
  }

  private static void addJsonPath(@NotNull SQL sql, @NotNull List<@NotNull String> path, int end) {
    sql.add("(jsondata");
    for (int i = 0; i < end; i++) {
      final String pname = path.get(i);
      sql.add("->");
      sql.addLiteral(pname);
    }
    sql.add(')');
  }

  private static class ToJsonB {
    ToJsonB(Object value) {
      this.value = value;
    }

    final Object value;
  }

  private static void addPropertyQuery(@NotNull SQL sql, @NotNull POp propertyOp, @NotNull List<Object> parameter) {
    final OpType op = propertyOp.op();
    if (POpType.AND == op || POpType.OR == op || POpType.NOT == op) {
      final List<@NotNull POp> children = propertyOp.children();
      if (children == null || children.size() == 0) {
        return;
      }
      final String op_literal;
      if (POpType.AND == op) {
        op_literal = " AND";
      } else if (POpType.OR == op) {
        op_literal = " OR";
      } else {
        op_literal = " NOT";
      }
      boolean first = true;
      sql.add('(');
      for (final @NotNull POp child : children) {
        if (first) {
          first = false;
        } else {
          sql.add(op_literal);
        }
        addPropertyQuery(sql, child, parameter);
      }
      sql.add(")");
      return;
    }
    sql.add(' ');
    final PRef pref = propertyOp.getPropertyRef();
    assert pref != null;
    final List<@NotNull String> path = pref.getPath();
    if (pref.getTagName() != null) {
      if (op != POpType.EXISTS) {
        throw new IllegalArgumentException("Tags do only support EXISTS operation, not " + op);
      }
      addJsonPath(sql, path, path.size());
      sql.add(" ?? ?");
      parameter.add(pref.getTagName());
      return;
    }
    if (op == POpType.EXISTS) {
      addJsonPath(sql, path, path.size() - 1);
      sql.add(" ?? ?");
      parameter.add(path.get(path.size() - 1));
      return;
    }
    final Object value = propertyOp.getValue();
    if (op == POpType.STARTS_WITH) {
      if (value instanceof String) {
        String text = (String) value;
        sql.add("(");
        addJsonPath(sql, path, path.size());
        sql.add("::text) LIKE ?");
        parameter.add(text + '%');
        return;
      }
      throw new IllegalArgumentException("STARTS_WITH operator requires a string as value");
    }
    addJsonPath(sql, path, path.size());
    if (op == POpType.EQ) {
      sql.add(" = ");
    } else if (op == POpType.GT) {
      sql.add(" > ");
    } else if (op == POpType.GTE) {
      sql.add(" >= ");
    } else if (op == POpType.LT) {
      sql.add(" < ");
    } else if (op == POpType.LTE) {
      sql.add(" <= ");
    } else {
      throw new IllegalArgumentException("Unknown operation: " + op);
    }
    sql.add("?::jsonb");
    try (final Json jp = Json.get()) {
      final PGobject jsonb = new PGobject();
      jsonb.setType("jsonb");
      jsonb.setValue(jp.writer().writeValueAsString(value));
      parameter.add(jsonb);
    } catch (SQLException | JsonProcessingException e) {
      throw unchecked(e);
    }
  }

  @NotNull
  Result executeRead(@NotNull ReadRequest<?> readRequest) {
    if (readRequest instanceof ReadFeatures) {
      final ReadFeatures readFeatures = (ReadFeatures) readRequest;
      final List<@NotNull String> collections = readFeatures.getCollections();
      if (collections.size() == 0) {
        return new PsqlSuccess(null);
      }
      final SQL sql = sql();
      final ArrayList<byte[]> wkbs = new ArrayList<>();
      final ArrayList<Object> parameters = new ArrayList<>();
      SOp spatialOp = readFeatures.getSpatialOp();
      if (spatialOp != null) {
        addSpatialQuery(sql, spatialOp, wkbs);
      }
      final String spatial_where = sql.toString();
      sql.setLength(0);
      POp propertyOp = readFeatures.getPropertyOp();
      if (propertyOp != null) {
        addPropertyQuery(sql, propertyOp, parameters);
      }
      final String props_where = sql.toString();
      sql.setLength(0);
      for (final String collection : collections) {
        // r_op text, r_id text, r_uuid text, r_type text, r_ptype text, r_feature jsonb, r_geometry geometry,
        // r_err jsonb
        sql.add("SELECT 'READ',\n" + "naksha_feature_id(jsondata),\n"
                + "naksha_feature_uuid(jsondata),\n"
                + "naksha_feature_type(jsondata),\n"
                + "naksha_feature_ptype(jsondata),\n"
                + "jsondata,\n"
                + "ST_AsEWKB(geo),\n"
                + "null FROM ")
            .addIdent(collection);
        if (spatial_where.length() > 0 || props_where.length() > 0) {
          sql.add(" WHERE");
          if (spatial_where.length() > 0) {
            sql.add(spatial_where);
            if (props_where.length() > 0) {
              sql.add(" AND");
            }
          }
          if (props_where.length() > 0) {
            sql.add(props_where);
          }
          if (readRequest.limit != null) {
            sql.add(" LIMIT ").add(readRequest.limit);
          }
        }
      }
      final String query = sql.toString();
      final PreparedStatement stmt = prepareStatement(query);
      try {
        int i = 1;
        for (final byte[] wkb : wkbs) {
          stmt.setBytes(i++, wkb);
        }
        for (final Object value : parameters) {
          if (value == null) {
            stmt.setString(i++, null);
          } else if (value instanceof PGobject) {
            stmt.setObject(i++, value);
          } else if (value instanceof String) {
            stmt.setString(i++, (String) value);
          } else if (value instanceof Double) {
            stmt.setDouble(i++, (Double) value);
          } else if (value instanceof Float) {
            stmt.setFloat(i++, (Float) value);
          } else if (value instanceof Long) {
            stmt.setLong(i++, (Long) value);
          } else if (value instanceof Integer) {
            stmt.setInt(i++, (Integer) value);
          } else if (value instanceof Short) {
            stmt.setShort(i++, (Short) value);
          } else if (value instanceof Boolean) {
            stmt.setBoolean(i++, (Boolean) value);
          } else {
            throw new IllegalArgumentException("Invalid value at index " + i + ": " + value);
          }
        }
        final ResultSet rs = stmt.executeQuery();
        final PsqlCursor<XyzFeature, XyzFeatureCodec> cursor =
            new PsqlCursor<>(XyzFeatureCodecFactory.get(), this, stmt, rs);
        return new PsqlSuccess(cursor);
      } catch (SQLException e) {
        try {
          stmt.close();
        } catch (SQLException ce) {
          log.atInfo()
              .setMessage("Failed to close statement")
              .setCause(ce)
              .log();
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
          "SELECT r_op, r_id, r_uuid, r_type, r_ptype, r_feature, r_geometry, r_err FROM naksha_write_collections(?);\n");
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
        final ResultSet rs = stmt.executeQuery();
        return new PsqlSuccess(new PsqlCursor<>(XyzCollectionCodecFactory.get(), this, stmt, rs), null);
      } catch (Throwable e) {
        try {
          stmt.close();
        } catch (Throwable ce) {
          log.atInfo()
              .setMessage("Failed to close statement")
              .setCause(ce)
              .log();
        }
        throw unchecked(e);
      }
    }
    if (writeRequest instanceof WriteFeatures<?, ?, ?>) {
      final WriteFeatures<?, ?, ?> writeFeatures = (WriteFeatures<?, ?, ?>) writeRequest;
      final int partition_id = -1;
      //      if (writeFeatures instanceof PostgresWriteFeaturesToPartition<?> writeToPartition) {
      //        partition_id = writeToPartition.partitionId;
      //      } else {
      //        partition_id = -1;
      //      }
      final PreparedStatement stmt = prepareStatement(
          "SELECT r_op, r_id, r_uuid, r_type, r_ptype, r_feature, ST_AsEWKB(r_geometry), r_err\n"
              + "FROM nk_write_features(?,?,?,?,?,?,?,?,?);");
      // nk_write_features(col_id, part_id, ops, ids, uuids, features, geometries, min_result, errors_only
      try (final Json json = Json.get()) {
        // new array list, so we don't modify original order
        final List<@NotNull CODEC> features = new ArrayList<>(writeRequest.features);
        features.forEach(codec -> codec.decodeParts(false));
        final Map<String, Integer> originalFeaturesOrder =
            IndexHelper.createKeyIndexMap(features, CODEC::getId);
        // sort to avoid deadlock
        features.sort(comparing(FeatureCodec::getId));

        final int SIZE = writeRequest.features.size();
        final String collection_id = writeFeatures.getCollectionId();
        // partition_id
        final String[] op_arr = new String[SIZE];
        final String[] id_arr = new String[SIZE];
        final String[] uuid_arr = new String[SIZE];
        final String[] json_arr = new String[SIZE];
        final byte[][] geo_arr = new byte[SIZE][];
        final boolean min_result = writeFeatures.minResults;
        final boolean err_only = false;

        final PostgresWriteOp out = new PostgresWriteOp();
        for (int i = 0; i < SIZE; i++) {
          final CODEC codec = features.get(i);
          op_arr[i] = codec.getOp();
          id_arr[i] = codec.getId();
          uuid_arr[i] = codec.getUuid();
          json_arr[i] = codec.getJson();
          geo_arr[i] = codec.getWkb();
        }
        stmt.setString(1, collection_id);
        stmt.setInt(2, partition_id);
        stmt.setArray(3, psqlConnection.createArrayOf("text", op_arr));
        stmt.setArray(4, psqlConnection.createArrayOf("text", id_arr));
        stmt.setArray(5, psqlConnection.createArrayOf("text", uuid_arr));
        stmt.setArray(6, psqlConnection.createArrayOf("jsonb", json_arr));
        stmt.setArray(7, psqlConnection.createArrayOf("bytea", geo_arr));
        stmt.setBoolean(8, min_result);
        stmt.setBoolean(9, err_only);
        final ResultSet rs = stmt.executeQuery();
        final PsqlCursor<FEATURE, CODEC> cursor =
            new PsqlCursor<>(writeRequest.getCodecFactory(), this, stmt, rs);
        try (final PreparedStatement err_stmt = prepareStatement("SELECT naksha_err_no(), naksha_err_msg();")) {
          final ResultSet err_rs = err_stmt.executeQuery();
          err_rs.next();
          final String errNo = err_rs.getString(1);
          final String errMsg = err_rs.getString(2);
          if (errNo != null) {
            return new PsqlError(XyzErrorMapper.psqlCodeToXyzError(errNo), errMsg, cursor);
          }
        }
        return new PsqlSuccess(cursor, originalFeaturesOrder);
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
