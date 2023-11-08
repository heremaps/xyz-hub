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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.EExecutedOp;
import com.here.naksha.lib.core.util.CloseableResource;
import com.here.naksha.lib.core.util.json.Json;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PostgresCursor extends CloseableResource<PostgresSession> {

  private static final Logger log = LoggerFactory.getLogger(PostgresCursor.class);

  PostgresCursor(
      @NotNull PsqlCursor<?> proxy,
      @NotNull PostgresSession session,
      @NotNull Statement stmt,
      @NotNull ResultSet rs) {
    super(proxy, session);
    this.stmt = stmt;
    this.rs = rs;
  }

  private @NotNull PostgresSession session() {
    final PostgresSession session = parent();
    assert session != null;
    return session;
  }

  private final @NotNull Statement stmt;
  private final @NotNull ResultSet rs;
  private boolean isValidPosition;
  private boolean isLoaded;
  // All result sets are:
  // (r_op text, r_id text, r_uuid text, r_type text, r_ptype text, r_feature jsonb, r_geometry geometry)
  private static final int R_OP = 1;
  private @Nullable String r_op;
  private static final int R_ID = 2;
  private @Nullable String r_id;
  private static final int R_UUID = 3;
  private @Nullable String r_uuid;
  private static final int R_TYPE = 4;
  private @Nullable String r_type;
  private static final int R_PTYPE = 5;
  private @Nullable String r_ptype;
  private static final int R_FEATURE = 6;
  private @Nullable String r_feature;
  private static final int R_GEOMETRY = 7;
  byte @Nullable [] r_geo;
  /**
   * The executed operation as enumeration value.
   */
  private @Nullable EExecutedOp op;
  /**
   * The cached parsed feature.
   */
  private @Nullable Object feature;
  /**
   * The cached parsed geometry.
   */
  private @Nullable Geometry geometry;

  @Override
  protected void destruct() {
    try {
      rs.close();
    } catch (SQLException e) {
      log.info("Failed to close result-set", e);
    }
    try {
      stmt.close();
    } catch (SQLException e) {
      log.info("Failed to close statement", e);
    }
  }

  void clear() {
    if (isLoaded) {
      r_op = null;
      r_id = null;
      r_uuid = null;
      r_type = null;
      r_ptype = null;
      r_feature = null;
      r_geo = null;
      // Clear cached parsed variants.
      feature = null;
      geometry = null;
      isLoaded = false;
    }
    isValidPosition = false;
  }

  /**
   * Load the raw values, if not already done.
   *
   * @throws SQLException           If any error occurred.
   * @throws NoSuchElementException If the cursor is at an invalid position.
   */
  void load() throws SQLException {
    if (!isValidPosition) {
      throw new NoSuchElementException();
    }
    if (!isLoaded) {
      r_op = rs.getString(R_OP);
      r_id = rs.getString(R_ID);
      r_uuid = rs.getString(R_UUID);
      r_type = rs.getString(R_TYPE);
      r_ptype = rs.getString(R_PTYPE); // may be null
      r_feature = rs.getString(R_FEATURE); // may be null
      r_geo = rs.getBytes(R_GEOMETRY); // may be null
      // Note: Only r_ptype, r_feature and r_geo may be null!
      assert r_op != null && r_id != null && r_uuid != null && r_type != null;
      // Note: The cached variants (feature & geometry) should always be null at this point!
      op = EExecutedOp.get(EExecutedOp.class, r_op);
      assert feature == null && geometry == null;
      isLoaded = true;
    }
  }

  boolean hasNext() {
    boolean isOnNext = false;
    try {
      if (rs.next()) {
        isOnNext = true;
        rs.previous();
        return true;
      }
      return false;
    } catch (SQLException e) {
      if (isOnNext) {
        // The exception happened when calling previous(), which is really evil.
        // We are now in a state that the caller does not expected, and we can't go back.
        isValidPosition = false;
        isLoaded = false;
        log.atError()
            .setMessage("Failed previous() call, result-set is now in unexpected state")
            .setCause(e)
            .log();
      } else {
        log.atInfo().setMessage("Failed next() call").setCause(e).log();
      }
      return false;
    }
  }

  boolean next() throws SQLException {
    clear();
    return isValidPosition = rs.next();
  }

  public boolean previous() throws SQLException {
    clear();
    return isValidPosition = rs.previous();
  }

  void beforeFirst() throws SQLException {
    clear();
    rs.beforeFirst();
  }

  boolean first() throws SQLException {
    clear();
    return isValidPosition = rs.first();
  }

  void afterLast() throws SQLException {
    clear();
    rs.afterLast();
  }

  boolean last() throws SQLException {
    clear();
    return rs.last();
  }

  boolean relative(long amount) throws SQLException {
    clear();
    while (amount > Integer.MAX_VALUE && rs.relative(Integer.MAX_VALUE)) {
      amount -= Integer.MAX_VALUE;
    }
    return isValidPosition = rs.relative((int) amount);
  }

  boolean absolute(long position) throws SQLException {
    clear();
    if (position > Integer.MAX_VALUE) {
      rs.beforeFirst();
      return isValidPosition = relative(position);
    }
    return isValidPosition = rs.absolute((int) position);
  }

  @NotNull
  EExecutedOp op() throws SQLException, NoSuchElementException {
    load();
    assert op != null;
    return op;
  }

  @NotNull
  String id() throws SQLException, NoSuchElementException {
    load();
    assert r_id != null;
    return r_id;
  }

  @NotNull
  String uuid() throws SQLException, NoSuchElementException {
    load();
    assert r_uuid != null;
    return r_uuid;
  }

  @NotNull
  String type() throws SQLException, NoSuchElementException {
    load();
    assert r_type != null;
    return r_type;
  }

  @Nullable
  String propertiesType() throws SQLException, NoSuchElementException {
    load();
    return r_ptype;
  }

  @Nullable
  String rawFeature() throws SQLException, NoSuchElementException {
    load();
    return r_feature;
  }

  byte @Nullable [] rawGeometry() throws SQLException, NoSuchElementException {
    load();
    return r_geo;
  }

  @Nullable
  <T> T getFeature(@NotNull Class<T> featureClass)
      throws SQLException, JsonMappingException, JsonProcessingException {
    load();
    if (r_feature == null) {
      return null;
    }
    if (feature == null || !featureClass.isInstance(featureClass)) {
      try (Json json = Json.get()) {
        feature = json.reader().forType(featureClass).readValue(r_feature);
        if (feature instanceof final XyzFeature xyzFeature) {
          final Geometry geometry = getGeometry();
          if (geometry != null) {
            xyzFeature.setGeometry(JTSHelper.fromGeometry(geometry));
          }
        }
      }
    }
    return featureClass.cast(feature);
  }

  @Nullable
  Geometry getGeometry() throws SQLException {
    load();
    if (geometry == null && r_geo != null) {
      try (Json json = Json.get()) {
        geometry = json.wkbReader.read(r_geo);
      } catch (ParseException e) {
        // TODO: Should be throw an exception?
        log.atWarn()
            .setMessage("Failed to deserialize geometry")
            .setCause(e)
            .log();
      }
    }
    return geometry;
  }
}
