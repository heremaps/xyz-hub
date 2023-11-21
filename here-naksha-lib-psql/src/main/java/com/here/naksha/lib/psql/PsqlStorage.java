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

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaAdminCollection;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.storage.*;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Future;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Naksha PostgresQL storage client. This client does implement low level access to manage collections and the features within these
 * collections. It as well grants access to transactions.
 */
@SuppressWarnings({"unused", "SqlResolve"})
public final class PsqlStorage implements IStorage {

  public static final String ADMIN_STORAGE_ID = "naksha-admin";

  private static final Logger log = LoggerFactory.getLogger(PsqlStorage.class);

  private static @NotNull PsqlDataSource dataSourceFromStorage(final @NotNull Storage storage) {
    final PsqlStorageProperties properties =
        JsonSerializable.convert(storage.getProperties(), PsqlStorageProperties.class);
    PsqlDataSource ds = null;
    if (properties.getDbConfig() != null) {
      ds = new PsqlDataSource(properties.getDbConfig());
    } else if (properties.getUrl() != null) {
      final PsqlConfig psqlConfig = new PsqlConfigBuilder()
          .withAppName("naksha")
          .parseUrl(properties.getUrl())
          .withDefaultSchema(NakshaAdminCollection.SCHEMA)
          .build();
      ds = new PsqlDataSource(psqlConfig);
    } else {
      throw new UnsupportedOperationException(
          "Psql DB configuration not available for storage id " + storage.getId());
    }
    return ds;
  }

  /**
   * The constructor to create a new PostgresQL storage client using a storage configuration.
   *
   * @param naksha  The Naksha-Hub reference.
   * @param storage The storage configuration to use for this client.
   * @throws SQLException If any error occurred while accessing the database.
   * @throws IOException  If reading the SQL extensions from the resources fail.
   */
  public PsqlStorage(@NotNull INaksha naksha, @NotNull Storage storage) throws SQLException, IOException {
    this(storage);
    this.naksha = naksha;
  }

  /**
   * The constructor to create a new PostgresQL storage client using a storage configuration.
   *
   * @param storage the storage configuration to use for this client.
   * @throws SQLException if any error occurred while accessing the database.
   * @throws IOException  if reading the SQL extensions from the resources fail.
   */
  public PsqlStorage(@NotNull Storage storage) throws SQLException, IOException {
    this(storage.getId(), dataSourceFromStorage(storage));
  }

  /**
   * Constructor to manually create a new PostgresQL storage client.
   *
   * @param config        The PSQL configuration to use for this client.
   * @param storageNumber The unique 40-bit unsigned integer storage number to use. Except for the main database (which always has the
   *                      number 0), normally this number is given by the Naksha-Hub, when creating a storage.
   * @throws SQLException If any error occurred while accessing the database.
   * @throws IOException  If reading the SQL extensions from the resources fail.
   */
  @Deprecated
  public PsqlStorage(@NotNull PsqlConfig config, long storageNumber) {
    this(config, Long.toString(storageNumber, 10));
  }

  /**
   * Constructor to manually create a new PostgresQL storage client.
   *
   * @param config    The PSQL configuration to use for this client; can be created using the {@link PsqlConfigBuilder}.
   * @param storageId The storage identifier.
   * @throws SQLException If any error occurred while accessing the database.
   * @throws IOException  If reading the SQL extensions from the resources fail.
   */
  public PsqlStorage(@NotNull PsqlConfig config, @NotNull String storageId) {
    this(storageId, new PsqlDataSource(config));
  }

  private PsqlStorage(@NotNull String storageId, @NotNull PsqlDataSource dataSource) {
    this.storage = new PostgresStorage(this, storageId, dataSource);
  }

  /**
   * The implementation.
   */
  private final @NotNull PostgresStorage storage;

  @NotNull
  PostgresStorage storage() {
    return storage.assertNotClosed();
  }

  /**
   * Returns the PostgresQL connection pool.
   *
   * @return the PostgresQL connection pool.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull PsqlPool getPsqlPool() {
    return getDataSource().getPool();
  }

  /**
   * Returns the PSQL data source.
   *
   * @return the PSQL data source.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull PsqlDataSource getDataSource() {
    return storage().dataSource;
  }

  /**
   * Returns the main schema to operate on.
   *
   * @return the main schema to operate on.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull String getSchema() {
    return storage().getSchema();
  }

  /**
   * Returns the storage identifier.
   *
   * @return the storage identifier.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull String getStorageId() {
    return storage().storageId;
  }

  /**
   * Returns the connector identification number.
   *
   * @return the connector identification number.
   */
  @Deprecated
  public long getStorageNumber() {
    return 0L;
  }

  @Override
  public void startMaintainer() {}

  @Override
  public void maintainNow() {}

  @Override
  public void stopMaintainer() {}

  @Deprecated
  @Override
  public void init() {
    initStorage();
  }

  /**
   * Ensure that the administration tables exists, and the Naksha extension script installed in the latest version.
   *
   * @throws SQLException If any error occurred while accessing the database.
   * @throws IOException  If reading the SQL extensions from the resources fail.
   */
  @Override
  public void initStorage() {
    storage().initStorage(false);
  }

  /**
   * Special method that installs the extension again with the latest pl/pgsql code coming together with this code and enabling debugging,
   * this will slow down the storage methods, but get a lot of debug information printed to PostgresQL logs (in DBeaver use Strg+Shit+O to
   * open the debug log output).
   */
  public void initStorageWithDebugInfo() {
    storage().initStorage(true);
  }

  /**
   * Drop the schema to which the storage is configured.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public void dropSchema() {
    storage().dropSchema();
  }

  @Override
  public @NotNull PsqlWriteSession newWriteSession(@Nullable NakshaContext context, boolean useMaster) {
    return storage().newWriteSession(context, useMaster);
  }

  @Override
  public @NotNull PsqlReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    return storage().newReadSession(context, useMaster);
  }

  @Deprecated
  public static int maxHistoryAgeInDays = 30;

  /**
   * Review all collections and ensure that the history does have the needed partitions created. The method will as well garbage collect the
   * history; if the history of a collection holds data that is too old (exceeds the maximum age), it deletes it.
   *
   * @throws SQLException If any error occurred.
   */
  @Deprecated
  @Override
  public void maintain(@NotNull List<CollectionInfo> collectionInfoList) {
    for (CollectionInfo collectionInfo : collectionInfoList) {
      try (final Connection conn = storage().dataSource.getConnection()) {
        try (final Statement stmt = conn.createStatement()) {
          stmt.execute(createHstPartitionOfOneDay(0, collectionInfo));
          stmt.execute(createHstPartitionOfOneDay(1, collectionInfo));
          stmt.execute(createHstPartitionOfOneDay(2, collectionInfo));
          stmt.execute(createTxPartitionOfOneDay(0));
          stmt.execute(createTxPartitionOfOneDay(1));
          stmt.execute(createTxPartitionOfOneDay(2));
          /*
          stmt.execute(deleteHstPartitionOfOneDay(maxHistoryAgeInDays, collectionInfo));
          stmt.execute(deleteHstPartitionOfOneDay(maxHistoryAgeInDays + 1, collectionInfo));
          stmt.execute(deleteHstPartitionOfOneDay(maxHistoryAgeInDays + 2, collectionInfo));
          stmt.execute(deleteHstPartitionOfOneDay(maxHistoryAgeInDays + 3, collectionInfo));
          stmt.execute(deleteHstPartitionOfOneDay(maxHistoryAgeInDays + 4, collectionInfo));
          stmt.execute(deleteHstPartitionOfOneDay(maxHistoryAgeInDays + 5, collectionInfo));
          stmt.execute(deleteTxPartitionOfOneDay(maxHistoryAgeInDays));
          stmt.execute(deleteTxPartitionOfOneDay(maxHistoryAgeInDays + 1));
          stmt.execute(deleteTxPartitionOfOneDay(maxHistoryAgeInDays + 2));
          stmt.execute(deleteTxPartitionOfOneDay(maxHistoryAgeInDays + 3));
          stmt.execute(deleteTxPartitionOfOneDay(maxHistoryAgeInDays + 4));
          stmt.execute(deleteTxPartitionOfOneDay(maxHistoryAgeInDays + 5));
          */
        }
        // commit once for every single collection so that partial progress is saved in case
        // something fails
        // midway
        conn.commit();
      } catch (Throwable t) {
        throw unchecked(t);
      }
    }
  }

  @Deprecated
  private String createHstPartitionOfOneDay(int dayPlus, CollectionInfo collectionInfo) {
    return new StringBuilder()
        .append("SELECT ")
        .append(getSchema())
        .append(".__naksha_create_hst_partition_for_day('")
        .append(collectionInfo.getId())
        .append("',current_timestamp+'")
        .append(dayPlus)
        .append(" day'::interval);")
        .toString();
  }

  @Deprecated
  private String createTxPartitionOfOneDay(int dayPlus) {
    return new StringBuilder()
        .append("SELECT ")
        .append(getSchema())
        .append(".__naksha_create_tx_partition_for_day(current_timestamp+'")
        .append(dayPlus)
        .append(" day'::interval);")
        .toString();
  }

  @Deprecated
  private String deleteHstPartitionOfOneDay(int dayOld, CollectionInfo collectionInfo) {
    return new StringBuilder()
        .append("SELECT ")
        .append(getSchema())
        .append(".__naksha_delete_hst_partition_for_day('")
        .append(collectionInfo.getId())
        .append("',current_timestamp-'")
        .append(dayOld)
        .append(" day'::interval);")
        .toString();
  }

  @Deprecated
  private String deleteTxPartitionOfOneDay(int dayOld) {
    return new StringBuilder()
        .append("SELECT ")
        .append(getSchema())
        .append(".__naksha_delete_tx_partition_for_day(current_timestamp-'")
        .append(dayOld)
        .append(" day'::interval);")
        .toString();
  }

  /**
   * Create default transaction settings.
   *
   * @return New transaction settings.
   */
  @Deprecated
  public @NotNull ITransactionSettings createSettings() {
    final PostgresStorage storage = storage();
    final PsqlPool pool = storage.dataSource.getPool();
    return new PsqlTransactionSettings(pool.config.stmtTimeout, pool.config.lockTimeout);
  }

  @Deprecated
  @Override
  public @NotNull PsqlTxReader openReplicationTransaction(@NotNull ITransactionSettings settings) {
    return new PsqlTxReader(this, settings);
  }

  @Deprecated
  @Override
  public @NotNull PsqlTxWriter openMasterTransaction(@NotNull ITransactionSettings settings) {
    return new PsqlTxWriter(this, settings);
  }

  private @Nullable INaksha naksha;

  @Override
  public @NotNull <T> Future<T> shutdown(@Nullable Fe1<T, IStorage> onShutdown) {
    return new PsqlShutdownTask<>(this, onShutdown, naksha, NakshaContext.currentContext()).start();
  }
}
