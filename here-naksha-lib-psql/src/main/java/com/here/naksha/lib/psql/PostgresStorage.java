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
import static com.here.naksha.lib.psql.SQL.quote_ident;
import static com.here.naksha.lib.psql.SQL.shouldEscape;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.util.CloseableResource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The internal implementation of a PSQL storage, it does not have any parent, it is a root resource.
 */
final class PostgresStorage extends CloseableResource<PostgresStorage> {

  private static final Logger log = LoggerFactory.getLogger(PostgresStorage.class);

  PostgresStorage(@NotNull PsqlStorage proxy, @NotNull String storageId, @NotNull PsqlDataSource dataSource) {
    super(proxy, null);
    this.storageId = storageId;
    this.dataSource = dataSource;
  }

  /**
   * The latest pl/pgsql extension version.
   */
  final @NotNull NakshaVersion latest = new NakshaVersion(2, 0, 7);
  // TODO: Increment me, when you change the pl/pgsql code and do not remove this TODO!!!

  /**
   * The storage identification.
   */
  final @NotNull String storageId;

  /**
   * The data source.
   */
  final @NotNull PsqlDataSource dataSource;

  @Override
  protected void destruct() {}

  @NotNull
  String getSchema() {
    return dataSource.getSchema();
  }

  @SuppressWarnings("SqlSourceToSinkFlow")
  synchronized void initStorage() {
    assertNotClosed();
    String SQL;
    final PsqlStorageConfig config = dataSource.getConfig();
    // Note: We need to open a "raw connection", so one, that is not initialized!
    //       The reason is, that the normal initialization would invoke naksha_init_plv8(),
    //       but init-storage is called to install exactly this method.
    try (final Connection conn = dataSource.getPool().dataSource.getConnection()) {
      try (final Statement stmt = conn.createStatement()) {
        long installed_version = 0L;
        try {
          final StringBuilder sb = new StringBuilder();
          sb.append("SELECT ");
          final String schema = getSchema();
          if (shouldEscape(schema)) {
            quote_ident(sb, getSchema());
          } else {
            sb.append(schema);
          }
          sb.append(".naksha_version();");
          final ResultSet rs = stmt.executeQuery(sb.toString());
          if (rs.next()) {
            installed_version = rs.getLong(1);
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
        if (config.logLevel == EPsqlLogLevel.VERBOSE || latest.toLong() > installed_version) {
          if (installed_version == 0L) {
            log.atInfo()
                .setMessage("Install and initialize Naksha extension v{}")
                .addArgument(latest)
                .log();
          } else {
            log.atInfo()
                .setMessage("Upgrade Naksha extension from v{} to v{}")
                .addArgument(new NakshaVersion(installed_version))
                .addArgument(latest)
                .log();
          }
          SQL = readResource("naksha_plpgsql.sql");
          if (config.logLevel == EPsqlLogLevel.VERBOSE) {
            SQL = SQL.replaceAll("--RAISE ", "RAISE ");
            SQL = SQL.replaceAll("--DEBUG ", " ");
          }
          SQL = SQL.replaceAll("\n--#", "\n");
          SQL = SQL.replaceAll("\nCREATE OR REPLACE FUNCTION nk__________.*;\n", "\n");
          SQL = SQL.replaceAll("\\$\\{schema}", getSchema());
          SQL = SQL.replaceAll(
              "\\$\\{version}",
              config.logLevel == EPsqlLogLevel.VERBOSE ? "0" : Long.toString(latest.toLong(), 10));
          SQL = SQL.replaceAll("\\$\\{storage_id}", storageId);
          //noinspection SqlSourceToSinkFlow
          stmt.execute(SQL);
          conn.commit();

          // Now, we can be sure that the code exists, and we can invoke it.
          // Note: We do not want to naksha_start_session to be invoked, therefore pass null!
          dataSource.initConnection(conn, null);
          stmt.execute("SELECT naksha_init();");
          conn.commit();
        }
      }
    } catch (Throwable t) {
      throw unchecked(t);
    }
  }

  void dropSchema() {
    try (final Connection conn = dataSource.getPool().dataSource.getConnection()) {
      try (final Statement stmt = conn.createStatement()) {
        try {
          final String sql = "DROP SCHEMA IF EXISTS " + SQL.quote_ident(getSchema()) + " CASCADE";
          stmt.execute(sql);
          conn.commit();
        } catch (PSQLException e) {
          final EPsqlState state = EPsqlState.of(e);
          if (state != EPsqlState.INVALID_SCHEMA_DEFINITION && state != EPsqlState.INVALID_SCHEMA_NAME) {
            throw e;
          }
          log.atInfo()
              .setMessage("Schema {} does not exist")
              .addArgument(getSchema())
              .log();
        }
      }
    } catch (Throwable t) {
      throw unchecked(t);
    }
  }

  @NotNull
  PsqlWriteSession newWriteSession(@Nullable NakshaContext context, boolean useMaster) {
    if (context == null) {
      context = NakshaContext.currentContext();
    }
    try {
      return new PsqlWriteSession(this, context, dataSource.getConnection());
    } catch (Exception e) {
      throw unchecked(e);
    }
  }

  @NotNull
  PsqlReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    if (context == null) {
      context = NakshaContext.currentContext();
    }
    try {
      return new PsqlReadSession(this, context, dataSource.getConnection());
    } catch (Exception e) {
      throw unchecked(e);
    }
  }
}
