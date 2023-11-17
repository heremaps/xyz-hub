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

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A configuration of a {@link PsqlStorage}, which does use connections from the connection pool of {@link PsqlInstance}'s. In fact, each
 * storage is a virtual cluster of PostgresQL nodes.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@SuppressWarnings("unused")
public class PsqlStorageConfig {

  public static final String HOST = "host";
  public static final String PORT = "port";
  public static final String DB = "db";
  public static final String USER = "user";
  public static final String PASSWORD = "password";
  public static final String CONN_TIMEOUT = "connTimeout";
  public static final String STMT_TIMEOUT = "stmtTimeout";
  public static final String LOCK_TIMEOUT = "lockTimeout";
  public static final String MIN_POOL_SIZE = "minPoolSize";
  public static final String MAX_POOL_SIZE = "maxPoolSize";
  public static final String IDLE_TIMEOUT = "idleTimeout";
  public static final String MAX_BANDWIDTH_IN_GBIT = "maxBandwidthInGBit";
  public static final String MEDIUM_LATENCY_IN_MILLIS = "mediumLatencyInMillis";
  public static final String SCHEMA = "schema";
  public static final String APP_NAME = "appName";

  public static final String ROLE = "role";

  public static final String LOG_LEVEL = "logLevel";
  public static final String SEARCH_PATH = "searchPath";

  @JsonCreator
  PsqlStorageConfig(
      @JsonProperty(HOST) @NotNull String host,
      @JsonProperty(PORT) Integer port,
      @JsonProperty(DB) @NotNull String db,
      @JsonProperty(USER) @NotNull String user,
      @JsonProperty(PASSWORD) @NotNull String password,
      @JsonProperty(CONN_TIMEOUT) Long connTimeout,
      @JsonProperty(STMT_TIMEOUT) Long stmtTimeout,
      @JsonProperty(LOCK_TIMEOUT) Long lockTimeout,
      @JsonProperty(MIN_POOL_SIZE) Integer minPoolSize,
      @JsonProperty(MAX_POOL_SIZE) Integer maxPoolSize,
      @JsonProperty(MAX_BANDWIDTH_IN_GBIT) Long maxBandwidthInGBit,
      @JsonProperty(MEDIUM_LATENCY_IN_MILLIS) Long mediumLatencyInMillis,
      @JsonProperty(IDLE_TIMEOUT) Long idleTimeout,
      @JsonProperty(SCHEMA) @NotNull String schema,
      @JsonProperty(APP_NAME) @NotNull String appName,
      @JsonProperty(ROLE) @Nullable String role,
      @JsonProperty(LOG_LEVEL) @Nullable EPsqlLogLevel logLevel,
      @JsonProperty(SEARCH_PATH) @Nullable String searchPath) {
    super(
        host,
        port,
        db,
        user,
        password,
        connTimeout,
        stmtTimeout,
        lockTimeout,
        minPoolSize,
        maxPoolSize,
        maxBandwidthInGBit,
        mediumLatencyInMillis,
        idleTimeout);
    this.schema = schema;
    this.appName = appName;
    this.role = role;
    this.logLevel = logLevel == null ? EPsqlLogLevel.OFF : logLevel;
    this.searchPath = searchPath != null && !searchPath.isEmpty() ? searchPath : null;
  }

  /**
   * Create a new PostgresQL configuration from a pool configuration.
   *
   * @param poolConfig The pool configuration upon which this PostgresQL configuration is based.
   * @param schema     The database schema to use.
   * @param appName    The application name to set, when connecting to the database.
   */
  public PsqlStorageConfig(@NotNull PsqlInstanceConfig poolConfig, @NotNull String schema, @NotNull String appName) {
    this(poolConfig, schema, appName, null, null, null);
  }

  /**
   * Create a new PostgresQL configuration from a pool configuration.
   *
   * @param poolConfig The pool configuration upon which this PostgresQL configuration is based.
   * @param schema     The database schema to use.
   * @param appName    The application name to set, when connecting to the database.
   * @param role       The role to use after connection; if {@code null}, then the {@link #user} is used.
   */
  public PsqlStorageConfig(
      @NotNull PsqlInstanceConfig poolConfig,
      @NotNull String schema,
      @NotNull String appName,
      @Nullable String role) {
    this(poolConfig, schema, appName, role, null, null);
  }

  /**
   * Create a new PostgresQL configuration from a pool configuration.
   *
   * @param poolConfig The pool configuration upon which this PostgresQL configuration is based.
   * @param schema     The database schema to use.
   * @param appName    The application name to set, when connecting to the database.
   * @param role       The role to use after connection; if {@code null}, then the {@link #user} is used.
   * @param logLevel   The log-level for debugging; if any.
   * @param searchPath The search path to set; if {@code null}, automatically set.
   */
  public PsqlStorageConfig(
      @NotNull PsqlInstanceConfig poolConfig,
      @NotNull String schema,
      @NotNull String appName,
      @Nullable String role,
      @Nullable EPsqlLogLevel logLevel,
      @Nullable String searchPath) {
    super(
        poolConfig.host,
        poolConfig.port,
        poolConfig.db,
        poolConfig.user,
        poolConfig.password,
        poolConfig.connTimeout,
        poolConfig.stmtTimeout,
        poolConfig.lockTimeout,
        poolConfig.minPoolSize,
        poolConfig.maxPoolSize,
        poolConfig.maxBandwidthInGBit,
        poolConfig.mediumLatencyInMillis,
        poolConfig.idleTimeout);
    this.schema = schema;
    this.appName = appName;
    this.role = role;
    this.logLevel = logLevel == null ? EPsqlLogLevel.OFF : logLevel;
    this.searchPath = searchPath;
  }

  /**
   * The data-source configuration backing this storage configuration.
   */
  public final @NotNull PsqlInstanceConfig dataSourceConfig;

  /**
   * The database schema to use.
   */
  public final @NotNull String schema;

  /**
   * The application name to set, when connecting to the database.
   */
  public final @NotNull String appName;

  /**
   * The role to use after connecting; if {@code null}, then the {@link #user} is used. This is no longer supported, because role switching
   * comes with major side effects. The current PostgresQL implementation will ignore the role.
   */
  public final @Nullable String role;

  /**
   * If the code is used for debugging purpose and if so, in which detail.
   */
  public final @NotNull EPsqlLogLevel logLevel;

  /**
   * The search path to set; if {@code null}, automatically set.
   */
  public final @Nullable String searchPath;

  // Check: https://www.postgresql.org/docs/current/runtime-config-client.html

  /**
   * The timeout in milliseconds when trying to establish a new connection to the database.
   */
  @JsonProperty
  public final long connTimeout;

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
  public final long stmtTimeout;

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
  public final long lockTimeout;


  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public @NotNull SELF withHost(String host) {
    setHost(host);
    return self();
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public @NotNull SELF withPort(int port) {
    setPort(port);
    return self();
  }

  public String getDb() {
    return db;
  }

  public void setDb(String db) {
    this.db = db;
  }

  public @NotNull SELF withDb(String db) {
    setDb(db);
    return self();
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public @NotNull SELF withUser(String user) {
    setUser(user);
    return self();
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public @NotNull SELF withPassword(String password) {
    setPassword(password);
    return self();
  }

  public long getConnTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(connTimeout, MILLISECONDS);
  }

  public void setConnTimeout(long connTimeout, @NotNull TimeUnit timeUnit) {
    this.connTimeout = MILLISECONDS.convert(connTimeout, timeUnit);
  }

  public @NotNull SELF withConnTimeout(long connTimeout, @NotNull TimeUnit timeUnit) {
    setConnTimeout(connTimeout, timeUnit);
    return self();
  }

  public long getStatementTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(stmtTimeout, MILLISECONDS);
  }

  public void setStatementTimeout(long stmtTimeout, @NotNull TimeUnit timeUnit) {
    this.stmtTimeout = MILLISECONDS.convert(stmtTimeout, timeUnit);
  }

  public @NotNull SELF withStatementTimeout(long stmtTimeout, @NotNull TimeUnit timeUnit) {
    setStatementTimeout(stmtTimeout, timeUnit);
    return self();
  }

  public long getLockTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(lockTimeout, MILLISECONDS);
  }

  public void setLockTimeout(long lockTimeout, @NotNull TimeUnit timeUnit) {
    this.lockTimeout = MILLISECONDS.convert(lockTimeout, timeUnit);
  }

  public @NotNull SELF withLockTimeout(long lockTimeout, @NotNull TimeUnit timeUnit) {
    setLockTimeout(lockTimeout, timeUnit);
    return self();
  }

  public int getMinPoolSize() {
    return minPoolSize;
  }

  public void setMinPoolSize(int minPoolSize) {
    this.minPoolSize = minPoolSize;
  }

  public @NotNull SELF withMinPoolSize(int minPoolSize) {
    setMinPoolSize(minPoolSize);
    return self();
  }

  public int getMaxPoolSize() {
    return maxPoolSize;
  }

  public void setMaxPoolSize(int maxPoolSize) {
    this.maxPoolSize = maxPoolSize;
  }

  public @NotNull SELF withMaxPoolSize(int maxPoolSize) {
    setMaxPoolSize(maxPoolSize);
    return self();
  }

  public long getMediumLatency(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(mediumLatencyInMillis, MILLISECONDS);
  }

  public void setMediumLatency(long latency, @NotNull TimeUnit timeUnit) {
    this.mediumLatencyInMillis = Math.min(PsqlInstanceConfig.MIN_LATENCY_MILLIS, MILLISECONDS.convert(latency, timeUnit));
  }

  public @NotNull SELF withMediumLatency(long latency, @NotNull TimeUnit timeUnit) {
    setMediumLatency(idleTimeout, timeUnit);
    return self();
  }

  public long getMaxBandwidthInGbit() {
    return maxBandwidthInGbit;
  }

  public void setMaxBandwidthInGbit(long maxBandwidthInGbit) {
    this.maxBandwidthInGbit = Math.min(PsqlInstanceConfig.MIN_BANDWIDTH_GBIT, maxBandwidthInGbit);
  }

  public @NotNull SELF withMaxBandwidthInGbit(long maxBandwidthInGbit) {
    setMaxBandwidthInGbit(maxBandwidthInGbit);
    return self();
  }

  public long getIdleTimeout(@NotNull TimeUnit timeUnit) {
    return timeUnit.convert(idleTimeout, MILLISECONDS);
  }

  public void setIdleTimeout(long idleTimeout, @NotNull TimeUnit timeUnit) {
    this.idleTimeout = MILLISECONDS.convert(idleTimeout, timeUnit);
  }

  public @NotNull SELF withIdleTimeout(long idleTimeout, @NotNull TimeUnit timeUnit) {
    setIdleTimeout(idleTimeout, timeUnit);
    return self();
  }

  public @Nullable EPsqlLogLevel getLogLevel() {
    return logLevel;
  }

  public void setLogLevel(@Nullable EPsqlLogLevel logLevel) {
    this.logLevel = logLevel;
  }

  public @NotNull SELF withLogLevel(@Nullable EPsqlLogLevel logLevel) {
    setLogLevel(logLevel);
    return self();
  }
}