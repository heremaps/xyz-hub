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

import static com.here.naksha.lib.core.NakshaContext.currentLogger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.here.naksha.lib.core.models.geojson.implementation.Feature;
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
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A Naksha PostgresQL transaction that can be used to read data, optionally using a read-replica, if opened as read-only transaction.
 */
public class PsqlTxReader implements IReadTransaction {

  /**
   * Creates a new transaction for the given PostgresQL client.
   *
   * @param psqlClient The PostgresQL client for which to create a new transaction.
   * @param settings The transaction settings.
   * @throws SQLException if creation of the reader failed.
   */
  PsqlTxReader(@NotNull PsqlStorage psqlClient, @NotNull ITransactionSettings settings) throws SQLException {
    this.psqlClient = psqlClient;
    this.settings = PsqlTransactionSettings.of(settings, this);
    this.connection = psqlClient.dataSource.getConnection(this.settings);
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
   * @return The underlying PostgresQL connection.
   * @throws IllegalStateException When the connection is closed.
   */
  public @NotNull Connection connection() {
    final Connection connection = this.connection;
    if (connection == null) {
      throw new IllegalStateException("Connection is closed");
    }
    return connection;
  }

  @NotNull
  PreparedStatement preparedStatement(@NotNull String sql) throws SQLException {
    return connection().prepareStatement(sql);
  }

  @NotNull
  Statement createStatement() throws SQLException {
    return connection().createStatement();
  }

  /**
   * Returns the client to which the transaction is bound.
   *
   * @return the client to which the transaction is bound.
   */
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
  public @NotNull String transactionNumber() throws SQLException {
    throw new UnsupportedOperationException("getTransactionNumber");
  }

  @Override
  public @NotNull ITransactionSettings settings() {
    return settings;
  }

  @Override
  public @NotNull ClosableIterator<@NotNull CollectionInfo> iterateCollections() throws SQLException {
    try (final var stmt = connection().prepareStatement(UtCollectionInfoResultSet.STATEMENT)) {
      return new UtCollectionInfoResultSet(stmt.executeQuery());
    }
  }

  @Override
  public @Nullable CollectionInfo getCollectionById(@NotNull String id) throws SQLException {
    final String SQL = "SELECT naksha_collection_get(?);";
    try (final var stmt = connection().prepareStatement(SQL)) {
      stmt.setString(1, id);
      final ResultSet rs = stmt.executeQuery();
      if (rs.next()) {
        final String jsonText = rs.getString(1);
        if (jsonText != null) {
          try (final Json json = Json.open()) {
            return json.reader(ViewDeserialize.Storage.class)
                .forType(CollectionInfo.class)
                .readValue(jsonText);
          } catch (JsonProcessingException e) {
            throw new SQLException("Failed to parse JSON", e);
          }
        }
      }
      return null;
    }
  }

  @Override
  public @NotNull <F extends Feature> PsqlFeatureReader<F> readFeatures(
      @NotNull Class<F> featureClass, @NotNull CollectionInfo collection) {
    // TODO: Optimize by tracking the read, no need to create a new instance for every call!
    return new PsqlFeatureReader<>(this, featureClass, collection);
  }

  @Override
  public void close() {
    if (connection != null) {
      try {
        connection.rollback();
      } catch (SQLException e) {
        currentLogger().info("Failed to execute rollback on JDBC connection", e);
      }
      try {
        connection.close();
      } catch (SQLException e) {
        currentLogger().info("Failed to close a JDBC connection", e);
      }
      connection = null;
    }
  }
}
