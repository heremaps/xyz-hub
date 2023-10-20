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
import static com.here.naksha.lib.core.util.IoHelp.readResource;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Naksha PostgresQL storage client. This client does implement low level access to manage collections and the features within these
 * collections. It as well grants access to transactions.
 */
@SuppressWarnings({"unused", "SqlResolve"})
public class PsqlStorage implements IStorage {

  private static final Logger log = LoggerFactory.getLogger(PsqlStorage.class);

  /**
   * The constructor to create a new PostgresQL storage client using a storage configuration.
   *
   * @param storage the storage configuration to use for this client.
   * @throws SQLException if any error occurred while accessing the database.
   * @throws IOException  if reading the SQL extensions from the resources fail.
   */
  public PsqlStorage(@NotNull Storage storage) throws SQLException, IOException {
    final PsqlStorageProperties properties =
        JsonSerializable.convert(storage.getProperties(), PsqlStorageProperties.class);
    this.dataSource = new PsqlDataSource(properties.getConfig());
    this.storageId = storage.getId();
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
    this.dataSource = new PsqlDataSource(config);
    this.storageId = storageId;
  }

  /**
   * Returns the PostgresQL connection pool.
   *
   * @return the PostgresQL connection pool.
   */
  public final @NotNull PsqlPool getPsqlPool() {
    return dataSource.getPool();
  }

  /**
   * The data source.
   */
  protected final @NotNull PsqlDataSource dataSource;

  /**
   * Returns the PSQL data source.
   *
   * @return the PSQL data source.
   */
  public final @NotNull PsqlDataSource getDataSource() {
    return dataSource;
  }

  /**
   * Returns the main schema to operate on.
   *
   * @return the main schema to operate on.
   */
  public final @NotNull String getSchema() {
    return dataSource.getSchema();
  }

  /**
   * Returns the storage identifier.
   *
   * @return the storage identifier.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public @NotNull String getStorageId() {
    return storageId;
  }

  /**
   * The storage identification.
   */
  protected final @NotNull String storageId;

  /**
   * Returns the connector identification number.
   *
   * @return the connector identification number.
   */
  @Deprecated
  public final long getStorageNumber() {
    return 0L;
  }

  @Deprecated
  NakshaVersion latest = NakshaVersion.latest;

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
    String SQL;
    try (final Connection conn = dataSource.getPool().dataSource.getConnection()) {
      try (final Statement stmt = conn.createStatement()) {
        long version = 0L;
        try {
          final StringBuilder sb = new StringBuilder();
          sb.append("SELECT ");
          com.here.naksha.lib.psql.SQL.quote_ident(sb, getSchema());
          sb.append(".naksha_version();");
          final ResultSet rs = stmt.executeQuery(sb.toString());
          if (rs.next()) {
            version = rs.getLong(1);
          }
          rs.close();
        } catch (PSQLException e) {
          final EPsqlState state = EPsqlState.of(e);
          if (state != EPsqlState.UNDEFINED_FUNCTION
              && state != EPsqlState.INVALID_SCHEMA_DEFINITION
              && state != EPsqlState.INVALID_SCHEMA_NAME) {
            throw e;
          }
          conn.rollback();
          log.atInfo()
              .setMessage("Naksha schema and/or extension missing")
              .log();
        }
        if (latest.toLong() != version) {
          if (version == 0L) {
            log.atInfo()
                .setMessage("Install and initialize Naksha extension v{}")
                .addArgument(latest)
                .log();
          } else {
            log.atInfo()
                .setMessage("Upgrade Naksha extension from v{} to v{}")
                .addArgument(new NakshaVersion(version))
                .addArgument(latest)
                .log();
          }
          SQL = readResource("naksha_plpgsql.sql");
          SQL = SQL.replace("${NAKSHA_PLV8_CODE}", readResource("naksha_plv8.js"));
          SQL = SQL.replace("${naksha_plv8_alweber}", readResource("naksha_plv8_alweber.js"));
          SQL = SQL.replace("${naksha_plv8_pawel}", readResource("naksha_plv8_pawel.js"));
          SQL = SQL.replaceAll("\\$\\{schema}", getSchema());
          SQL = SQL.replaceAll("\\$\\{storage_id}", getStorageId());
          stmt.execute(SQL);
          conn.commit();

          // Re-Initialize the connection.
          // This ensures that we really have the schema at the end of the search path and therefore
          // selected.
          // TODO HP_QUERY : Looks like a duplicate call, as initialization already happens during
          // earlier call to getConnection()?
          dataSource.initConnection(conn);
          stmt.execute("SELECT naksha_init();");
          conn.commit();
        }
      }
    } catch (Throwable t) {
      throw unchecked(t);
    }
  }

  // TODO HP_QUERY : History for admin collections?
  public static int maxHistoryAgeInDays = 30; // TODO this or Space.maxHistoryAge

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
      try (final Connection conn = dataSource.getConnection()) {
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
    return new PsqlTransactionSettings(
        dataSource.getPool().config.stmtTimeout, dataSource.getPool().config.lockTimeout);
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
}
