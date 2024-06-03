/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.naksha.lib.core.NakshaVersion.latest;
import static com.here.naksha.lib.core.exceptions.UncheckedException.cause;
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.psql.SQL.quote_ident;
import static com.here.naksha.lib.psql.SQL.shouldEscapeIdent;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.exceptions.StorageNotInitialized;
import com.here.naksha.lib.core.exceptions.Unauthorized;
import com.here.naksha.lib.core.util.ClosableRootResource;
import com.here.naksha.lib.core.util.IoHelp;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.jdbc.PgConnection;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The internal implementation of a PSQL storage, it does not have any parent, it is a root resource.
 */
final class PostgresStorage extends ClosableRootResource {

  private static final Logger log = LoggerFactory.getLogger(PostgresStorage.class);
  private static long MIN_CONN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(1);
  private static long DEFAULT_CONN_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(5);

  private static long MIN_STMT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(2);
  private static long DEFAULT_STMT_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(60);

  private static long MIN_LOCK_TIMEOUT_MILLIS = 100;
  private static long DEFAULT_LOCK_TIMEOUT_MILLIS = TimeUnit.SECONDS.toMillis(1);

  private static long value(@Nullable Long value, long minValue, long defaultValue) {
    if (value == null) {
      return defaultValue;
    }
    return Math.max(value, minValue);
  }

  PostgresStorage(
      @NotNull PsqlStorage proxy,
      @NotNull String storageId,
      @NotNull String appName,
      @NotNull String schema,
      @Nullable PsqlInstanceConfig masterConfig,
      @Nullable List<@NotNull PsqlInstanceConfig> readerConfigs,
      @Nullable Long connTimeout,
      @Nullable Long stmtTimeout,
      @Nullable Long lockTimeout,
      @Nullable EPsqlLogLevel logLevel) {
    super(proxy);
    this.storageId = storageId;
    this.schema = schema;
    this.appName = appName;
    this.connTimeout = value(connTimeout, MIN_CONN_TIMEOUT_MILLIS, DEFAULT_CONN_TIMEOUT_MILLIS);
    this.stmtTimeout = value(stmtTimeout, MIN_STMT_TIMEOUT_MILLIS, DEFAULT_STMT_TIMEOUT_MILLIS);
    this.lockTimeout = value(lockTimeout, MIN_LOCK_TIMEOUT_MILLIS, DEFAULT_LOCK_TIMEOUT_MILLIS);
    this.logLevel = logLevel == null ? EPsqlLogLevel.OFF : logLevel;
    this.masterConfig = masterConfig;
    if (masterConfig != null) {
      master.set(PsqlInstance.get(masterConfig));
    }
    this.readerConfigs = readerConfigs;
    if (readerConfigs != null) {
      for (final PsqlInstanceConfig readerConfig : readerConfigs) {
        final PsqlInstance readerInstance = PsqlInstance.get(readerConfig);
        if (!readers.contains(readerInstance)) {
          readers.add(readerInstance);
        }
      }
    }
  }

  public @NotNull String getSchema() {
    return schema;
  }

  public void setSchema(@NotNull String schema) {
    this.schema = schema;
  }

  public @NotNull String getAppName() {
    return appName;
  }

  public void setAppName(@NotNull String appName) {
    this.appName = appName;
  }

  public long getConnTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(connTimeout, MILLISECONDS);
  }

  public void setConnTimeout(long connTimeout, @NotNull TimeUnit timeUnit) {
    this.connTimeout = MILLISECONDS.convert(connTimeout, timeUnit);
  }

  public long getSocketTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(sockedReadTimeout, MILLISECONDS);
  }

  public void setSocketTimeout(long timeout, @NotNull TimeUnit timeUnit) {
    this.sockedReadTimeout = Math.max(2, MILLISECONDS.convert(timeout, timeUnit));
  }

  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(stmtTimeout, MILLISECONDS);
  }

  public void setStatementTimeout(long stmtTimeout, @NotNull TimeUnit timeUnit) {
    this.stmtTimeout = MILLISECONDS.convert(stmtTimeout, timeUnit);
  }

  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(lockTimeout, MILLISECONDS);
  }

  public void setLockTimeout(long lockTimeout, @NotNull TimeUnit timeUnit) {
    this.lockTimeout = MILLISECONDS.convert(lockTimeout, timeUnit);
  }

  public @NotNull EPsqlLogLevel getLogLevel() {
    return logLevel;
  }

  public void setLogLevel(@Nullable EPsqlLogLevel logLevel) {
    this.logLevel = logLevel == null ? EPsqlLogLevel.OFF : logLevel;
  }

  /**
   * The storage identification.
   */
  final @NotNull String storageId;

  /**
   * The configuration of the master node; if any.
   */
  @JsonProperty("master")
  @JsonInclude(Include.NON_NULL)
  private @Nullable PsqlInstanceConfig masterConfig;

  @JsonIgnore
  private final @NotNull AtomicReference<@Nullable PsqlInstance> master = new AtomicReference<>(null);

  /**
   * The configuration of the read-replicas; if any.
   */
  @JsonProperty("reader")
  @JsonInclude(Include.NON_EMPTY)
  private @Nullable List<@NotNull PsqlInstanceConfig> readerConfigs;

  @JsonIgnore
  private final @NotNull CopyOnWriteArrayList<@NotNull PsqlInstance> readers = new CopyOnWriteArrayList<>();

  /**
   * The database schema to use.
   */
  private @NotNull String schema;

  /**
   * The application name to set, when connecting to the database.
   */
  private @NotNull String appName;

  /**
   * If the code is used for debugging purpose and if so, in which detail.
   */
  private @NotNull EPsqlLogLevel logLevel;

  // Check: https://www.postgresql.org/docs/current/runtime-config-client.html

  /**
   * The timeout in milliseconds when trying to establish a new connection to the database.
   */
  @JsonProperty
  private long connTimeout;

  /**
   * Abort any statement that takes more than the specified amount of milliseconds. A value of zero (the default) disables the timeout.
   *
   * <p>The timeout is measured from the time a command arrives at the server until it is completed
   * by the server. If multiple SQL statements appear in a single simple-Query message, the timeout is applied to each statement separately.
   * (PostgreSQL versions before 13 usually treated the timeout as applying to the whole query string). In extended query protocol, the
   * timeout starts running when any query-related message (Parse, Bind, Execute, Describe) arrives, and it is canceled by completion of an
   * Execute or Sync message.
   */
  @JsonProperty
  private long stmtTimeout;

  /**
   * Abort any statement that waits longer than the specified amount of milliseconds while attempting to acquire a lock on a table, index,
   * row, or other database object. The time limit applies separately to each lock acquisition attempt. The limit applies both to explicit
   * locking requests (such as LOCK TABLE, or SELECT FOR UPDATE without NOWAIT) and to implicitly-acquired locks. A value of zero (the
   * default) disables the timeout.
   *
   * <p>Unlike statement_timeout, this timeout can only occur while waiting for locks. Note that if
   * statement_timeout is nonzero, it is rather pointless to set lock_timeout to the same or larger value, since the statement timeout would
   * always trigger first. If log_min_error_statement is set to ERROR or lower, the statement that timed out will be logged.
   *
   * <p>Setting lock_timeout in postgresql.conf is not recommended because it would affect all
   * sessions.
   */
  private long lockTimeout;

  // TODO: Add getter/setter, add value to constructor, to properties ...
  private int fetchSize = 1000;

  int getFetchSize() {
    return fetchSize;
  }

  private long sockedReadTimeout = TimeUnit.SECONDS.toMillis(15);

  /**
   * Cancel command is sent out of band over its own connection, so cancel message can itself get stuck. This property controls "connect
   * timeout" and "socket timeout" used for cancel commands. The timeout is specified in seconds. Default value is 10 seconds.
   *
   * <p>We do not yet expose this, because it can't be modified for existing connections.
   */
  private final long cancelSignalTimeout = TimeUnit.SECONDS.toMillis(15);

  @Override
  protected void destruct() {}

  /**
   * The default initializer for connections.
   *
   * @param conn    The connection.
   * @param context If a context is given, it must have a valid application-identifier and causes {@code naksha_start_session} to be
   *                invoked.
   * @return the connection.
   * @throws SQLException If the initialization failed.
   * @throws Unauthorized If a context is given and does not have a valid application-identifier.
   */
  @SuppressWarnings("SqlSourceToSinkFlow")
  @AvailableSince(NakshaVersion.v2_0_7)
  void initConnection(@NotNull PostgresConnection conn, @Nullable NakshaContext context) {
    try {
      final PgConnection pgConnection = conn.get();
      pgConnection.setAutoCommit(false);
      try (final Statement stmt = pgConnection.createStatement()) {
        final SQL sql = new SQL();
        initSession(sql, context);
        final String query = sql.toString();
        log.debug("{} - Init connection: {}", appName, query);
        stmt.execute(query);
        pgConnection.commit();
      }
    } catch (Exception e) {
      throw unchecked(e);
    }
  }

  /**
   * Generates the initialization query.
   *
   * <p><b>Note</b>: If SET (or equivalently SET SESSION) is issued within a transaction that is later aborted, the effects of the SET
   * command disappear when the transaction is rolled back. Once the surrounding transaction is committed, the effects will persist until
   * the end of the session, unless overridden by another SET, see the
   * <a href="https://www.postgresql.org/docs/current/sql-set.html">PostgresQL documentation</a>. Therefore, the query that is created in
   * this method is committed, because we do not know how many transactions are done with the connection, and we want all of them to use the
   * same settings.
   *
   * @param sql     The SQL builder in which to create the query.
   * @param context If a context is given, it must have a valid application-identifier and causes {@code naksha_start_session} to be
   *                invoked.
   * @throws Unauthorized If a context without application-identifier given.
   */
  void initSession(@NotNull SQL sql, @Nullable NakshaContext context) {
    sql.add("SET SESSION search_path TO ").addLiteral(schema).add(",topology,public;\n");
    sql.add("SET SESSION application_name TO ").addLiteral(appName).add(";\n");
    if (context != null) {
      sql.add("SELECT naksha_start_session(");
      sql.addLiteral(appName);
      sql.add(',');
      sql.addLiteral(context.getAppId());
      sql.add(',');
      final String author = context.getAuthor();
      if (author != null) {
        sql.addLiteral(author);
      } else {
        sql.add("null");
      }
      sql.add(',');
      sql.addLiteral(context.getStreamId());
      sql.add(");\n");
    }
    sql.add("SET SESSION work_mem TO '256 MB';\n");
    sql.add("SET SESSION enable_seqscan TO OFF;\n");
    sql.add("SET SESSION statement_timeout TO ").add(stmtTimeout).add(";\n");
    sql.add("SET SESSION lock_timeout TO ").add(lockTimeout).add(";\n");
  }

  @NotNull
  PsqlConnection getConnection(boolean useMaster, boolean readOnly, boolean init, @Nullable NakshaContext context)
      throws SQLException {
    final PsqlInstance psqlInstance;
    if (!useMaster && readOnly && readers.size() > 0) {
      psqlInstance = readers.get(0);
    } else {
      psqlInstance = master.get();
    }
    if (psqlInstance == null) {
      throw new SQLException("Unable to find a valid server");
    }
    final PsqlConnection psqlConnection =
        psqlInstance.getConnection(connTimeout, sockedReadTimeout, cancelSignalTimeout);
    if (!psqlConnection.postgresConnection.parent().config.readOnly) {
      // If this is a master connection, ensure that the read-only mode is set correctly.
      psqlConnection.postgresConnection.get().setReadOnly(readOnly);
    }
    if (init) {
      initConnection(psqlConnection.postgresConnection, context);
    }
    return psqlConnection;
  }

  @SuppressWarnings("SqlSourceToSinkFlow")
  synchronized void initStorage(@NotNull PsqlStorage.Params params, @NotNull IoHelp ioHelp) {
    assertNotClosed();
    String SQL;
    // Note: We need to open a "raw connection", so one, that is not initialized!
    //       The reason is, that the normal initialization would invoke naksha_init_plv8(),
    //       but init-storage is called to install exactly this method.
    try {
      try (final PsqlConnection conn = getConnection(true, false, false, null)) {
        try (final Statement stmt = conn.createStatement()) {
          long installed_version = 0L;
          try {
            final StringBuilder sb = new StringBuilder();
            sb.append("SELECT ");
            final String schema = getSchema();
            if (shouldEscapeIdent(schema)) {
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
            final EPsqlState state = EPsqlState.get(e);
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
          if (logLevel.toLong() > EPsqlLogLevel.OFF.toLong() || latest.toLong() > installed_version) {
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
            SQL = ioHelp.readResource(ioHelp.findResource("/naksha_plpgsql.sql", PostgresStorage.class));
            if (logLevel.toLong() >= EPsqlLogLevel.DEBUG.toLong()) {
              SQL = SQL.replaceAll("--RAISE ", "RAISE ");
              SQL = SQL.replaceAll("--DEBUG ", " ");
            }
            if (logLevel.toLong() >= EPsqlLogLevel.VERBOSE.toLong()) {
              SQL = SQL.replaceAll("--VERBOSE ", " ");
            }
            if (params.pg_hint_plan()) {
              SQL = SQL.replaceAll("--pg_hint_plan:", " ");
            }
            if (params.pg_stat_statements()) {
              SQL = SQL.replaceAll("--pg_stat_statements:", " ");
            }
            SQL = SQL.replaceAll("\n--#", "\n");
            SQL = SQL.replaceAll("\nCREATE OR REPLACE FUNCTION nk__________.*;\n", "\n");
            SQL = SQL.replaceAll("\\$\\{schema}", getSchema());
            SQL = SQL.replaceAll(
                "\\$\\{version}",
                logLevel.toLong() > EPsqlLogLevel.OFF.toLong()
                    ? "0"
                    : Long.toString(latest.toLong(), 10));
            SQL = SQL.replaceAll("\\$\\{storage_id}", storageId);
            //noinspection SqlSourceToSinkFlow
            stmt.execute(SQL);
            conn.commit();

            // Now, we can be sure that the code exists, and we can invoke it.
            // Note: We do not want to naksha_start_session to be invoked, therefore pass null!
            initConnection(conn.postgresConnection, null);
            stmt.execute("SELECT naksha_init();");
            conn.commit();
          }
        }
      }
    } catch (Throwable t) {
      throw unchecked(t);
    }
  }

  void dropSchema() {
    try (final Connection conn = getConnection(true, false, false, null)) {
      try (final Statement stmt = conn.createStatement()) {
        try {
          final String sql = "DROP SCHEMA IF EXISTS " + SQL.quote_ident(getSchema()) + " CASCADE";
          stmt.execute(sql);
          conn.commit();
        } catch (PSQLException e) {
          final EPsqlState state = EPsqlState.get(e);
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
      return new PsqlWriteSession(this, context, getConnection(true, false, true, context));
    } catch (Exception e) {
      throw wrapException(e);
    }
  }

  @NotNull
  PsqlReadSession newReadSession(@Nullable NakshaContext context, boolean useMaster) {
    if (context == null) {
      context = NakshaContext.currentContext();
    }
    try {
      return new PsqlReadSession(this, context, getConnection(useMaster, true, true, context));
    } catch (Exception e) {
      throw wrapException(e);
    }
  }

  private @NotNull RuntimeException wrapException(@NotNull Exception e) {
    final Throwable cause = cause(e);
    if (cause instanceof SQLException) {
      final EPsqlState psqlState = EPsqlState.get((SQLException) cause);
      if (psqlState == EPsqlState.NAKSHA_STORAGE_NOT_INITIALIZED || psqlState == EPsqlState.UNDEFINED_FUNCTION) {
        throw new StorageNotInitialized();
      }
    }
    return unchecked(e);
  }
}
