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

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.ClosableIterator;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IReadTransaction;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PSQLException;
import org.postgresql.util.PSQLState;

/**
 * A Naksha PostgresQL transaction that can be used to read data, optionally using a read-replica, if opened as read-only transaction.
 */
@Deprecated
public class PsqlTxReader implements IReadTransaction {

  /**
   * Creates a new transaction for the given PostgresQL client.
   *
   * @param psqlClient The PostgresQL client for which to create a new transaction.
   * @param settings   The transaction settings.
   * @throws SQLException if creation of the reader failed.
   */
  @SuppressWarnings("JavadocDeclaration")
  PsqlTxReader(@NotNull PsqlStorage psqlClient, @NotNull ITransactionSettings settings) {
    try {
      this.psqlClient = psqlClient;
      this.settings = PsqlTransactionSettings.of(settings, this);
      this.connection = null; // psqlClient.getDataSource().getConnection();
      naksha_tx_start();
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }

  protected boolean naksha_tx_start_write() {
    return false;
  }

  protected void naksha_tx_start() {
    try (final PreparedStatement stmt = preparedStatement("SELECT naksha_tx_start(?, ?, ?);")) {
      // This guarantees, that the search-path is okay.
      // psqlClient.getDataSource().initConnection(conn(), null);
      stmt.setString(1, settings.getAppId());
      stmt.setString(2, settings.getAuthor());
      stmt.setBoolean(3, naksha_tx_start_write());
      stmt.execute();
    } catch (final Exception e) {
      throw unchecked(e);
    }
  }

  /**
   * The transaction settings.
   */
  final @NotNull PsqlTransactionSettings settings;

  /**
   * The PostgresQL client to which this transaction is bound.
   */
  final @NotNull PsqlStorage psqlClient;

  /**
   * The JDBC connection.
   */
  @Nullable
  Connection connection;

  /**
   * Returns the underlying PostgresQL connection.
   *
   * @return The underlying PostgresQL connection.
   * @throws PSQLException When the connection is closed.
   */
  public @NotNull Connection conn() {
    final Connection connection = this.connection;
    if (connection == null) {
      throw unchecked(new PSQLException("Connection is closed", PSQLState.CONNECTION_DOES_NOT_EXIST));
    }
    return connection;
  }

  @NotNull
  PreparedStatement preparedStatement(@NotNull String sql) throws SQLException {
    return conn().prepareStatement(sql);
  }

  @NotNull
  Statement createStatement() throws SQLException {
    return conn().createStatement();
  }

  /**
   * Returns the client to which the transaction is bound.
   *
   * @return the client to which the transaction is bound.
   */
  @SuppressWarnings("unused")
  public @NotNull PsqlStorage getPsqlClient() {
    return psqlClient;
  }

  /**
   * Returns the underlying JDBC connection. Can be used to perform arbitrary queries.
   *
   * @return the underlying JDBC connection.
   * @throws IllegalStateException if the connection is already closed.
   */
  public @NotNull Connection getConnection() {
    final Connection connection = this.connection;
    if (connection == null) {
      throw new IllegalStateException("Connecton closed");
    }
    return connection;
  }

  @Override
  public @NotNull String transactionNumber() {
    throw new UnsupportedOperationException("getTransactionNumber");
  }

  @Override
  public @NotNull ITransactionSettings settings() {
    return settings;
  }

  @Override
  public @NotNull ClosableIterator<@NotNull CollectionInfo> iterateCollections() {
    try {
      final var stmt = conn().prepareStatement(UtCollectionInfoResultSet.STATEMENT);
      try {
        return new UtCollectionInfoResultSet(stmt, stmt.executeQuery());
      } catch (final Throwable t) {
        stmt.close();
        throw t;
      }
    } catch (Throwable t) {
      throw unchecked(t);
    }
  }

  @Override
  public @Nullable CollectionInfo getCollectionById(@NotNull String id) {
    final String SQL = "SELECT naksha_collection_get(?);";
    try {
      final var stmt = conn().prepareStatement(SQL);
      try {
        stmt.setString(1, id);
        final ResultSet rs = stmt.executeQuery();
        if (rs.next()) {
          final String jsonText = rs.getString(1);
          if (jsonText != null) {
            try (final Json json = Json.get()) {
              return json.reader(ViewDeserialize.Storage.class)
                  .forType(CollectionInfo.class)
                  .readValue(jsonText);
            }
          }
        }
        return null;
      } catch (Throwable t) {
        stmt.close();
        throw t;
      }
    } catch (final Throwable t) {
      throw unchecked(t);
    }
  }

  @SuppressWarnings("rawtypes")
  private final ConcurrentHashMap<Class, PsqlFeatureReader> cachedReaders = new ConcurrentHashMap<>();

  @SuppressWarnings("unchecked")
  @Override
  public @NotNull <F extends XyzFeature> PsqlFeatureReader<F, PsqlTxReader> readFeatures(
      @NotNull Class<F> featureClass, @NotNull CollectionInfo collection) {
    PsqlFeatureReader<F, PsqlTxReader> reader = cachedReaders.get(featureClass);
    if (reader != null) {
      return reader;
    }
    reader = new PsqlFeatureReader<>(this, featureClass, collection);
    final PsqlFeatureReader<F, PsqlTxReader> existing = cachedReaders.putIfAbsent(featureClass, reader);
    if (existing != null) {
      return existing;
    }
    return reader;
  }

  @Override
  public void close() {
    if (connection != null) {
      try {
        connection.rollback();
      } catch (final Throwable t) {
        currentLogger()
            .atWarn("Automatic rollback failed for JDBC connection")
            .setCause(t)
            .log();
      }
      try {
        connection.close();
      } catch (final Throwable t) {
        currentLogger()
            .atWarn("Automatic closing of PostgresQL connection failed")
            .setCause(t)
            .log();
      }
      connection = null;
    }
  }
}
