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

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.lambdas.Pe1;
import com.here.naksha.lib.core.models.TxSignalSet;
import com.here.naksha.lib.core.models.features.Storage;
import com.here.naksha.lib.core.storage.IStorage;
import com.here.naksha.lib.core.storage.ITransactionSettings;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PSQLException;

/**
 * The Naksha PostgresQL storage client. This client does implement low level access to manage collections and the features within these
 * collections. It as well grants access to transactions.
 */
public class PsqlStorage implements IStorage {

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
    this.dataSource = new PsqlDataSource(properties.config);
    this.storageNumber = storage.getNumber();
    init();
  }

  /**
   * Constructor to manually create a new PostgresQL storage client.
   *
   * @param config        the PSQL configuration to use for this client.
   * @param storageNumber the unique 40-bit unsigned integer storage number to use. Except for the main database (which always has the
   *                      number 0), normally this number is given by the Naksha-Hub, when creating a storage.
   * @throws SQLException if any error occurred while accessing the database.
   * @throws IOException  if reading the SQL extensions from the resources fail.
   */
  public PsqlStorage(@NotNull PsqlConfig config, long storageNumber) throws SQLException, IOException {
    this.dataSource = new PsqlDataSource(config);
    this.storageNumber = storageNumber;
  }

  /**
   * Returns the PostgresQL connection pool.
   *
   * @return the PostgresQL connection pool.
   */
  public final @NotNull PsqlPool getPsqlPool() {
    return dataSource.pool;
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
    return dataSource.schema;
  }

  /**
   * The connector identification number to use. Except for the main database (which always has the number 0), normally this number is given
   * by the Naksha-Hub, when creating a connector.
   */
  protected final long storageNumber;

  /**
   * Returns the connector identification number.
   *
   * @return the connector identification number.
   */
  public final long getStorageNumber() {
    return storageNumber;
  }

  static final boolean[] ESCAPE = new boolean[128];

  static {
    Arrays.fill(ESCAPE, true);
    for (int c = '0'; c <= '9'; c++) {
      ESCAPE[c] = false;
    }
    for (int c = 'a'; c <= 'z'; c++) {
      ESCAPE[c] = false;
    }
    for (int c = 'A'; c <= 'Z'; c++) {
      ESCAPE[c] = false;
    }
    ESCAPE['_'] = false;
  }

  /**
   * Tests if the given identifier must be escaped.
   *
   * @param id the identifier to test.
   * @return true if the identifier must be escaped; false otherwise.
   */
  static boolean shouldEscape(@NotNull CharSequence id) {
    for (int i = 0; i < id.length(); i++) {
      final char c = id.charAt(i);
      // We signal that every less than the space must be escaped. The escape method then will throw
      // an SQLException!
      if (c < 32 || c > 126 || ESCAPE[c]) {
        return true;
      }
    }
    return false;
  }

  static void escapeWrite(@NotNull CharSequence chars, @NotNull StringBuilder sb) throws SQLException {
    // See: https://www.asciitable.com/
    // We only allows characters between 32 (space) and 126 (~).
    for (int i = 0; i < chars.length(); i++) {
      final char c = chars.charAt(i);
      if (c < 32 || c > 126) {
        throw new SQLException("Illegal character in identifier: " + chars);
      }
      if (c == '"') {
        sb.append('"').append('"');
      } else {
        sb.append(c);
      }
    }
  }

  /**
   * Escape the given identifier.
   *
   * @param sb The string builder into which to write the escaped identifier.
   * @param id The identifier to escape.
   * @return The given string builder.
   * @throws SQLException If the identifier contains illegal characters, for example the ASCII-0.
   */
  static @NotNull StringBuilder escapeId(@NotNull StringBuilder sb, @NotNull CharSequence id) throws SQLException {
    sb.append('"');
    escapeWrite(id, sb);
    sb.append('"');
    return sb;
  }

  /**
   * Escape all given identifiers together, not individually, for example:
   *
   * <pre>{@code
   * String prefix = "hello";
   * String postfix = "world";
   * escapeId(prefix, "_", postfix);
   * }</pre>
   * <p>
   * will result in the code generated being: {@code "hello_world"}.
   *
   * @param sb  The string builder into which to write the escaped identifiers.
   * @param ids The identifiers to escape.
   * @return The given string builder.
   * @throws SQLException If any identifier contains illegal characters, for example the ASCII-0.
   */
  static @NotNull StringBuilder escapeId(@NotNull StringBuilder sb, @NotNull CharSequence... ids)
      throws SQLException {
    sb.append('"');
    for (final CharSequence id : ids) {
      escapeWrite(id, sb);
    }
    sb.append('"');
    return sb;
  }

  static final String C3P0EXT_CONFIG_SCHEMA = "config.schema()"; // TODO: Why to we need this?
  static final String[] extensionList = new String[]{"postgis", "postgis_topology", "tsm_system_rows", "dblink"};
  static final String[] localScripts = new String[]{"/xyz_ext.sql", "/h3Core.sql", "/naksha_ext.sql"};

  // We can store meta-information at tables using
  // COMMENT ON TABLE "xyz_config"."transactions" IS '{"id":"transactions"}';
  // SELECT pg_catalog.obj_description('xyz_config.transactions'::regclass, 'pg_class');
  // We should simply store the NakshaCollection information in serialized form.

  NakshaVersion latest = INaksha.latest;

  /**
   * Ensure that the administration tables exists, and the Naksha extension script installed in the latest version.
   *
   * @throws SQLException If any error occurred while accessing the database.
   * @throws IOException  If reading the SQL extensions from the resources fail.
   */
  public void init() throws SQLException, IOException {
    String SQL;
    try (final Connection conn = dataSource.getConnection()) {
      try (final Statement stmt = conn.createStatement()) {
        long version = 0L;
        try {
          ResultSet rs = stmt.executeQuery("SELECT naksha_version();");
          if (rs.next()) {
            version = rs.getLong(1);
          }
        } catch (PSQLException e) {
          if (EPsqlState.of(e) != EPsqlState.UNDEFINED_FUNCTION
              && EPsqlState.of(e) != EPsqlState.INVALID_SCHEMA_DEFINITION) {
            throw e;
          }
          conn.rollback();
          currentLogger().info("Naksha schema and/or extension missing");
        }
        if (latest.toLong() != version) {
          if (version == 0L) {
            currentLogger().info("Install and initialize Naksha extension v{}", latest);
          } else {
            currentLogger()
                .info("Upgrade Naksha extension from v{} to v{}", new NakshaVersion(version), latest);
          }
          SQL = IoHelp.readResource("naksha_ext.sql")
              .replaceAll("\\$\\{schema}", getSchema())
              .replaceAll("\\$\\{storage_id}", Long.toString(getStorageNumber()));
          stmt.execute(SQL);
          conn.commit();

          // Re-Initialize the connection.
          // This ensures that we really have the schema at the end of the search path and therefore selected.
          dataSource.initConnection(conn);
          if (version == 0L) {
            stmt.execute("SELECT naksha_init();");
            conn.commit();
          }
          //      setupH3();
        }
      }
    }
  }

  /**
   * Review all collections and ensure that the history does have the needed partitions created. The method will as well garbage collect the
   * history; if the history of a collection holds data that is too old (exceeds the maximum age), it deletes it.
   *
   * @throws SQLException If any error occurred.
   */
  @Override
  public void maintain() throws SQLException {
    throw new UnsupportedOperationException("maintain");
  }

  /**
   * Create default transaction settings.
   *
   * @return New transaction settings.
   */
  public @NotNull ITransactionSettings createSettings() {
    return new PsqlTransactionSettings(dataSource.pool.config.stmtTimeout, dataSource.pool.config.lockTimeout);
  }

  @Override
  public @NotNull PsqlTxReader openReplicationTransaction(@NotNull ITransactionSettings settings) throws SQLException {
    return new PsqlTxReader(this, settings);
  }

  @Override
  public @NotNull PsqlTxWriter openMasterTransaction(@NotNull ITransactionSettings settings) throws SQLException {
    return new PsqlTxWriter(this, settings);
  }

  @Override
  public void addListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {
    throw new UnsupportedOperationException("addListener");
  }

  @Override
  public boolean removeListener(@NotNull Pe1<@NotNull TxSignalSet> listener) {
    throw new UnsupportedOperationException("removeListener");
  }

  @Override
  public void close() {
  }

  // Add listener for change events like create/update/delete collection and for the features in it.
  // Basically, listen to transaction log!
  // https://www.postgresql.org/docs/current/sql-notify.html
  // https://access.crunchydata.com/documentation/pgjdbc/42.1.0/listennotify.html

  /**
   * Setup H3?
   *
   * @throws SQLException if any error occurred.
   */
  private void setupH3() throws SQLException {
    final PsqlPoolConfig config = dataSource.getConfig();
    try (final Connection connection = dataSource.getConnection();
        final Statement stmt = connection.createStatement()) {
      boolean needUpdate = false;
      ResultSet rs;
      if ((rs = stmt.executeQuery("select count(1)::integer from pg_catalog.pg_proc r inner join"
          + " pg_catalog.pg_namespace l  on ( r.pronamespace = l.oid ) where 1 = 1"
          + " and l.nspname = 'h3' and r.proname = 'h3_version'"))
          .next()) {
        needUpdate = (0 == rs.getInt(1));
      }

      //      if (!needUpdate && (rs = stmt.executeQuery("select h3.h3_version()")).next()) {
      //        needUpdate = (H3_CORE_VERSION > rs.getInt(1));
      //      }

      //      if (needUpdate) {
      //        stmt.execute(readResource("/h3Core.sql"));
      //        stmt.execute(MaintenanceSQL.generateSearchPathSQL(XYZ_CONFIG));
      //        logger.info("{} - Successfully created H3 SQL-Functions on database {}@{}",
      // clientName, config.user, config.url);
      //      }
    } catch (Exception e) { // IOException
      //      logger.error("{} - Failed run h3 init on database: {}@{}", clientName, config.user,
      // config.url, e);
      throw new SQLException("Failed to setup H3: " + e.getMessage());
    }
  }
}