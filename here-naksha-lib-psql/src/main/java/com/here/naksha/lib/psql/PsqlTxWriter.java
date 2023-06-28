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
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IMasterTransaction;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** A Naksha PostgresQL database transaction that can be used to read and mutate data. */
public class PsqlTxWriter extends PsqlTxReader implements IMasterTransaction {

  /**
   * Creates a new transaction for the given PostgresQL client.
   *
   * @param psqlClient the PostgresQL client for which to create a new transaction.
   * @throws SQLException if creation of the writer failed.
   */
  PsqlTxWriter(@NotNull PsqlStorage psqlClient) throws SQLException {
    super(psqlClient);
  }

  @Override
  public @NotNull CollectionInfo createCollection(@NotNull CollectionInfo collection) throws SQLException {
    try (final PreparedStatement stmt = preparedStatement("SELECT naksha_collection_upsert(?, ?, ?);")) {
      stmt.setString(1, collection.getId());
      stmt.setLong(2, collection.getMaxAge());
      stmt.setBoolean(3, collection.getHistory());
      final ResultSet rs = stmt.executeQuery();
      rs.next();
      try (final Json json = Json.open()) {
        return json.reader(ViewDeserialize.Storage.class)
            .forType(CollectionInfo.class)
            .readValue(rs.getString(1));
      } catch (JsonProcessingException e) {
        throw new SQLException(e);
      }
    }
  }

  @Override
  public @NotNull CollectionInfo updateCollection(@NotNull CollectionInfo collection) throws SQLException {
    throw new UnsupportedOperationException("updateCollection");
  }

  @AvailableSince(INaksha.v2_0_0)
  public @NotNull CollectionInfo upsertCollection(@NotNull CollectionInfo collection) throws SQLException {
    throw new UnsupportedOperationException("updateCollection");
  }

  @Override
  public @NotNull CollectionInfo deleteCollection(@NotNull CollectionInfo collection, long deleteAt)
      throws SQLException {
    throw new UnsupportedOperationException("dropCollection");
  }

  @Override
  @NotNull
  public CollectionInfo dropCollection(@NotNull CollectionInfo collection) throws Exception {
    try (final PreparedStatement stmt = preparedStatement("SELECT naksha_collection_drop(?);")) {
      stmt.setString(1, collection.getId());
      final ResultSet rs = stmt.executeQuery();
      rs.next();
      try (final var json = Json.open()) {
        return json.reader(ViewDeserialize.class)
            .forType(CollectionInfo.class)
            .readValue(rs.getString(1));
      }
    }
  }

  @Override
  public @NotNull CollectionInfo enableHistory(@NotNull CollectionInfo collection) throws SQLException {
    throw new UnsupportedOperationException("enableHistory");
  }

  @Override
  public @NotNull CollectionInfo disableHistory(@NotNull CollectionInfo collection) throws SQLException {
    throw new UnsupportedOperationException("disableHistory");
  }

  @Override
  public @NotNull <F extends Feature> PsqlFeatureWriter<F> writeFeatures(
      @NotNull Class<F> featureClass, @NotNull CollectionInfo collection) {
    // TODO: Optimize by tracking the write, no need to create a new instance for every call!
    return new PsqlFeatureWriter<>(this, featureClass, collection);
  }

  /**
   * Commit all changes.
   *
   * @throws SQLException If any error occurred.
   */
  public void commit() throws SQLException {
    //noinspection resource
    connection().commit();
  }

  /** Abort the transaction. */
  public void rollback() {
    try {
      //noinspection resource
      connection().rollback();
    } catch (SQLException ignore) {
    }
  }

  /** Close the transaction, which effectively will roll back the transaction. */
  @Override
  public void close() {
    rollback();
  }
}
