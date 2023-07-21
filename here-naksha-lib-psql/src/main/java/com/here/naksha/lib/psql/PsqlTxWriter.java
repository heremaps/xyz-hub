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

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IMasterTransaction;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * A Naksha PostgresQL database transaction that can be used to read and mutate data.
 */
public class PsqlTxWriter extends PsqlTxReader implements IMasterTransaction {

  /**
   * Creates a new transaction for the given PostgresQL client.
   *
   * @param psqlClient the PostgresQL client for which to create a new transaction.
   * @param settings   The transaction settings.
   * @throws SQLException if creation of the writer failed.
   */
  PsqlTxWriter(@NotNull PsqlStorage psqlClient, @NotNull ITransactionSettings settings) {
    super(psqlClient, settings);
  }

  @Override
  protected boolean naksha_tx_start_write() {
    return true;
  }

  @Override
  public @NotNull CollectionInfo createCollection(@NotNull CollectionInfo collection) {
    try (final PreparedStatement stmt = preparedStatement("SELECT naksha_collection_upsert(?, ?, ?);")) {
      stmt.setString(1, collection.getId());
      stmt.setLong(2, collection.getMaxAge());
      stmt.setBoolean(3, collection.getHistory());
      final ResultSet rs = stmt.executeQuery();
      rs.next();
      try (final Json json = Json.get()) {
        return json.reader(ViewDeserialize.Storage.class)
            .forType(CollectionInfo.class)
            .readValue(rs.getString(1));
      }
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }

  @Override
  public @NotNull CollectionInfo updateCollection(@NotNull CollectionInfo collection) {
    throw new UnsupportedOperationException("updateCollection");
  }

  @AvailableSince(NakshaVersion.v2_0_0)
  public @NotNull CollectionInfo upsertCollection(@NotNull CollectionInfo collection) {
    throw new UnsupportedOperationException("updateCollection");
  }

  @Override
  public @NotNull CollectionInfo deleteCollection(@NotNull CollectionInfo collection, long deleteAt)
      throws SQLException {
    throw new UnsupportedOperationException("dropCollection");
  }

  @Override
  @NotNull
  public CollectionInfo dropCollection(@NotNull CollectionInfo collection) {
    try (final PreparedStatement stmt = preparedStatement("SELECT naksha_collection_drop(?);")) {
      stmt.setString(1, collection.getId());
      final ResultSet rs = stmt.executeQuery();
      rs.next();
      try (final var json = Json.get()) {
        return json.reader(ViewDeserialize.class)
            .forType(CollectionInfo.class)
            .readValue(rs.getString(1));
      }
    } catch (Throwable t) {
      throw unchecked(t);
    }
  }

  @Override
  public @NotNull CollectionInfo enableHistory(@NotNull CollectionInfo collection) {
    throw new UnsupportedOperationException("enableHistory");
  }

  @Override
  public @NotNull CollectionInfo disableHistory(@NotNull CollectionInfo collection) {
    throw new UnsupportedOperationException("disableHistory");
  }

  @SuppressWarnings("rawtypes")
  private final ConcurrentHashMap<Class, PsqlFeatureWriter> cachedWriters = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  @Override
  public @NotNull <F extends XyzFeature> PsqlFeatureWriter<F> writeFeatures(
      @NotNull Class<F> featureClass, @NotNull CollectionInfo collection) {
    PsqlFeatureWriter<F> writer = cachedWriters.get(featureClass);
    if (writer != null) {
      return writer;
    }
    writer = new PsqlFeatureWriter<>(this, featureClass, collection);
    final PsqlFeatureWriter<F> existing = cachedWriters.putIfAbsent(featureClass, writer);
    if (existing != null) {
      return existing;
    }
    return writer;
  }

  /**
   * Commit all changes.
   *
   * @throws SQLException If any error occurred.
   */
  public void commit() {
    try {
      conn().commit();
    } catch (final Throwable t) {
      throw unchecked(t);
    } finally {
      // start a new transaction, this ensures that the app_id and author are set.
      naksha_tx_start();
    }
  }

  /**
   * Abort the transaction.
   */
  public void rollback() {
    try {
      conn().rollback();
    } catch (final Throwable t) {
      currentLogger().atWarn("Automatic rollback failed").setCause(t).log();
    } finally {
      // start a new transaction, this ensures that the app_id and author are set.
      naksha_tx_start();
    }
  }
}
