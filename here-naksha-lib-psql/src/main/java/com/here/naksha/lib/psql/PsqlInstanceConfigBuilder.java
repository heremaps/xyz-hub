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

import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import org.jetbrains.annotations.NotNull;

/**
 * A builder for a PostgresQL database instance configuration.
 */
@SuppressWarnings("unused")
public final class PsqlInstanceConfigBuilder extends PsqlAbstractConfigBuilder<PsqlInstanceConfig, PsqlInstanceConfigBuilder> {

  // TODO: Auto-Adjust connections using:
  // SELECT setting FROM pg_settings WHERE name = 'max_connections'; <-- max allowed connections
  // SELECT count(*) FROM pg_stat_activity; <-- current open connections
  // Auto-Adjust the IDLE timeout based upon how much open connections there are, as more, as shorted the timeout!

  public PsqlInstanceConfigBuilder() {
  }

  @Override
  public @NotNull PsqlInstanceConfig build() throws NullPointerException {
    if (db == null) {
      throw new NullPointerException("db");
    }
    if (user == null) {
      throw new NullPointerException("user");
    }
    if (password == null) {
      throw new NullPointerException("password");
    }
    return new PsqlInstanceConfig(host, port, db, user, password, readOnly);
  }

  int port = 5432;
  String host;
  String db;
  String user;
  String password;
  boolean readOnly;

  @Override
  protected void setBasics(@NotNull String host, int port, @NotNull String db) {
    setHost(host);
    setPort(port);
    setDb(db);
  }

  @Override
  protected void setParams(@NotNull QueryParameterList params) {
    if (params.getValue("user") instanceof String _user) {
      user = _user;
    }
    if (params.getValue("password") instanceof String _password) {
      password = _password;
    }
    if (params.contains("readOnly")) {
      final Object raw = params.getValue("readOnly");
      if (raw instanceof Boolean bool) {
        readOnly = bool;
      } else if (raw instanceof String string) {
        // This is read-only, except "&readOnly=false" or "&readOnly=no".
        readOnly = !"false".equalsIgnoreCase(string) && !"no".equalsIgnoreCase(string);
      } else if (raw instanceof Number number) {
        // This is read-only, except "&readOnly=0".
        readOnly = number.longValue() != 0L;
      } else {
        // "&readOnly".
        readOnly = true;
      }
    }
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public @NotNull PsqlInstanceConfigBuilder withHost(String host) {
    setHost(host);
    return this;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public @NotNull PsqlInstanceConfigBuilder withPort(int port) {
    setPort(port);
    return this;
  }

  public String getDb() {
    return db;
  }

  public void setDb(String db) {
    this.db = db;
  }

  public @NotNull PsqlInstanceConfigBuilder withDb(String db) {
    setDb(db);
    return this;
  }

  public String getUser() {
    return user;
  }

  public void setUser(String user) {
    this.user = user;
  }

  public @NotNull PsqlInstanceConfigBuilder withUser(String user) {
    setUser(user);
    return this;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public @NotNull PsqlInstanceConfigBuilder withPassword(String password) {
    setPassword(password);
    return this;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public @NotNull PsqlInstanceConfigBuilder withReadOnly(boolean isReadOnly) {
    setReadOnly(isReadOnly);
    return this;
  }

}
