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

import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class PsqlStorageConfigBuilder extends PsqlAbstractConfigBuilder<PsqlStorageConfig, PsqlStorageConfigBuilder> {

  @SuppressWarnings("PatternVariableHidesField")
  @Override
  protected void setParams(@NotNull QueryParameterList params) {
    super.setParams(params);
    if (params.getValue("schema") instanceof String schema) {
      this.schema = schema;
    }
    if (params.getValue("app") instanceof String app) {
      this.appName = app;
    }
    if (params.getValue("appName") instanceof String appName) {
      this.appName = appName;
    }
  }

  public String getSchema() {
    return schema;
  }

  public void setSchema(@NotNull String schema) {
    this.schema = schema;
  }

  public @NotNull PsqlStorageConfigBuilder withSchema(@NotNull String schema) {
    setSchema(schema);
    return this;
  }

  public @NotNull PsqlStorageConfigBuilder withSchemaIfAbsent(@NotNull String schema) {
    if (getSchema() == null) setSchema(schema);
    return this;
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(@NotNull String appName) {
    this.appName = appName;
  }

  public @NotNull PsqlStorageConfigBuilder withAppName(@NotNull String appName) {
    setAppName(appName);
    return this;
  }

  @Deprecated
  public String getRole() {
    return role;
  }

  @Deprecated
  public void setRole(String role) {
    this.role = role;
  }

  @Deprecated
  public @NotNull PsqlStorageConfigBuilder withRole(String role) {
    setRole(role);
    return this;
  }

  public String getSearchPath() {
    return searchPath;
  }

  public void setSearchPath(String searchPath) {
    this.searchPath = searchPath;
  }

  public @NotNull PsqlStorageConfigBuilder withSearchPath(String searchPath) {
    setSearchPath(searchPath);
    return this;
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

  /** The database schema to use. */
  private String schema;

  /** The application name to set, when connecting to the database. */
  private String appName;

  /** The role to use after connection; if {@code null}, then the {@link #user} is used. */
  @Deprecated
  private String role;

  /** The search path to set; if {@code null}, automatically set. */
  private String searchPath;


  // Hikari connection pool configuration
  long connTimeout;
  long stmtTimeout;
  long lockTimeout;
  EPsqlLogLevel logLevel;

  @Override
  public @NotNull PsqlStorageConfig build() {
    if (schema == null) {
      throw new NullPointerException("schema");
    }
    if (appName == null) {
      throw new NullPointerException("appName");
    }
    if (db == null) {
      throw new NullPointerException("db");
    }
    if (user == null) {
      throw new NullPointerException("user");
    }
    if (password == null) {
      throw new NullPointerException("password");
    }
    return new PsqlStorageConfig(
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
        maxBandwidthInGbit,
        mediumLatencyInMillis,
        idleTimeout,
        schema,
        appName,
        role,
        logLevel,
        searchPath);
  }
}
