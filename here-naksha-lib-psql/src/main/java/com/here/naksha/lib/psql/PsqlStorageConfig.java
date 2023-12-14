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

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.IoHelp.LoadedBytes;
import java.nio.charset.StandardCharsets;
import org.jetbrains.annotations.NotNull;

/**
 * A simple configuration object created from a URL of the following format:
 * <pre>{@code
 * jdbc:postgresql://{HOST}[:{PORT}]/{DB}
 *   ?user={USER}
 *   &password={PASSWORD}
 *   &id={STORAGE-ID}
 *   &schema={SCHEMA}
 *   &app={APPLICATION-NAME}
 *   [&readOnly[=true|false]]
 * }</pre>. All parameters, except for <i>readOnly</i> are mandatory.
 */
@SuppressWarnings("unused")
public class PsqlStorageConfig extends PsqlByUrlBuilder<PsqlStorageConfig> {

  /**
   * Reads the configuration from a configuration file from user home directory ({@code ~/.config/naksha/filename}) or from the environment
   * variable, if none is possible, a default localhost configuration is used.
   *
   * @param filename     The filename to search for in {@code ~/.config/naksha/}.
   * @param envName      The environment variable to check.
   * @param sharedSchema The shared schema, when using the shared environment variable {@code TEST_NAKSHA_PSQL_URL}.
   * @return the PSQL storage configuration.
   */
  @SuppressWarnings("SameParameterValue")
  public static @NotNull PsqlStorageConfig configFromFileOrEnv(
      @NotNull String filename, @NotNull String envName, @NotNull String sharedSchema) {
    try {
      final LoadedBytes loadedBytes = IoHelp.readBytesFromHomeOrResource(filename, true, "naksha");
      final byte[] bytes = loadedBytes.getBytes();
      String url = new String(bytes, StandardCharsets.UTF_8);
      if (url.startsWith("jdbc:postgresql://")) {
        return new PsqlStorageConfig(url).withStorageId(PsqlStorage.ADMIN_STORAGE_ID);
      }
    } catch (Exception ignore) {
    }
    String url = System.getenv(envName);
    if (url != null && url.startsWith("jdbc:postgresql://")) {
      return new PsqlStorageConfig(url).withStorageId(PsqlStorage.ADMIN_STORAGE_ID);
    }
    url = System.getenv("TEST_NAKSHA_PSQL_URL");
    if (url != null && url.startsWith("jdbc:postgresql://")) {
      return new PsqlStorageConfig(url)
          .withStorageId(PsqlStorage.ADMIN_STORAGE_ID)
          .withSchema(sharedSchema);
    }

    String password = System.getenv("TEST_NAKSHA_PSQL_PASS");
    if (password == null || password.isBlank()) {
      password = "password";
    }
    return new PsqlStorageConfig("jdbc:postgresql://localhost/postgres?user=postgres&password=" + password
        + "&schema=" + sharedSchema
        + "&app=" + "Naksha/v" + NakshaVersion.latest
        + "&id=" + PsqlStorage.ADMIN_STORAGE_ID);
  }

  public PsqlStorageConfig(@NotNull String url) {
    parseUrl(url);
  }

  @Override
  protected void setBasics(@NotNull String host, int port, @NotNull String db) {
    this.host = host;
    this.port = port;
    this.db = db;
  }

  @Override
  protected void setParams(@NotNull QueryParameterList params) {
    if (params.getValue("user") instanceof String) {
      user = (String) params.getValue("user");
    } else {
      throw new IllegalArgumentException("The URL must have a parameter '&user'");
    }
    if (params.getValue("password") instanceof String) {
      password = (String) params.getValue("password");
    } else {
      throw new IllegalArgumentException("The URL must have a parameter '&password'");
    }
    if (params.getValue("appName") instanceof String) {
      appName = (String) params.getValue("appName");
    } else if (params.getValue("appname") instanceof String) {
      appName = (String) params.getValue("appname");
    } else if (params.getValue("app_name") instanceof String) {
      appName = (String) params.getValue("app_name");
    } else if (params.getValue("app") instanceof String) {
      appName = (String) params.getValue("app");
    } else {
      throw new IllegalArgumentException("The URL must have a parameter '&app'");
    }
    if (params.getValue("schema") instanceof String) {
      schema = (String) params.getValue("schema");
    } else {
      throw new IllegalArgumentException("The URL must have a parameter '&schema'");
    }
    if (params.getValue("id") instanceof String) {
      storageId = (String) params.getValue("id");
    } else if (params.getValue("storageId") instanceof String) {
      storageId = (String) params.getValue("storageId");
    } else if (params.getValue("storageid") instanceof String) {
      storageId = (String) params.getValue("storageid");
    } else if (params.getValue("storage_id") instanceof String) {
      storageId = (String) params.getValue("storage_id");
    } else {
      throw new IllegalArgumentException("The URL must have a parameter '&id'");
    }
    Object raw = params.getValue("readOnly");
    if (raw == null) {
      raw = params.getValue("readonly");
    }
    if (raw instanceof Boolean) {
      readOnly = (Boolean) raw;
    } else if (raw instanceof String) {
      readOnly = !"false".equalsIgnoreCase((String) raw);
    } else {
      readOnly = false;
    }
  }

  private String url;

  public @NotNull String url() {
    if (url == null) {
      updateUrlAndMaster();
    }
    return url;
  }

  private String host;

  public @NotNull String host() {
    return host;
  }

  public @NotNull PsqlStorageConfig withHost(@NotNull String host) {
    this.url = null;
    this.host = host;
    return this;
  }

  private int port;

  public int port() {
    return port;
  }

  public @NotNull PsqlStorageConfig withPort(int port) {
    if (port < 0 || port > 65535) {
      throw new IllegalArgumentException("port must be between 0 and 65535, but is " + port);
    }
    this.url = null;
    this.port = port;
    return this;
  }

  private String db;

  public @NotNull String db() {
    return db;
  }

  public @NotNull PsqlStorageConfig withDb(@NotNull String db) {
    this.url = null;
    this.db = db;
    return this;
  }

  private String user;

  public @NotNull String user() {
    return user;
  }

  public @NotNull PsqlStorageConfig withUser(@NotNull String user) {
    this.url = null;
    this.user = user;
    return this;
  }

  private String password;

  public @NotNull String password() {
    return password;
  }

  public @NotNull PsqlStorageConfig withPassword(@NotNull String password) {
    this.url = null;
    this.password = password;
    return this;
  }

  private String storageId;

  public @NotNull String storageId() {
    return storageId;
  }

  public @NotNull PsqlStorageConfig withStorageId(@NotNull String storageId) {
    this.url = null;
    this.storageId = storageId;
    return this;
  }

  private String appName;

  public @NotNull String appName() {
    return appName;
  }

  public @NotNull PsqlStorageConfig withAppName(@NotNull String appName) {
    this.url = null;
    this.appName = appName;
    return this;
  }

  private String schema;

  public @NotNull String schema() {
    return schema;
  }

  public @NotNull PsqlStorageConfig withSchema(@NotNull String schema) {
    this.url = null;
    this.schema = schema;
    return this;
  }

  private boolean readOnly;

  public boolean readOnly() {
    return readOnly;
  }

  public @NotNull PsqlStorageConfig withReadOnly(boolean readOnly) {
    this.url = null;
    this.readOnly = readOnly;
    return this;
  }

  private PsqlInstanceConfig master;

  public @NotNull PsqlInstanceConfig master() {
    if (url == null) {
      updateUrlAndMaster();
    }
    return master;
  }

  void updateUrlAndMaster() {
    final StringBuilder sb = new StringBuilder();
    sb.append("jdbc:postgresql://");
    sb.append(host);
    if (port != 5432 && port != 0) {
      sb.append(':');
      sb.append(port);
    }
    sb.append('/').append(db);
    sb.append("?schema=").append(schema);
    sb.append("&id=").append(storageId);
    sb.append("&app=").append(appName);
    sb.append("&user=").append(user);
    sb.append("&password=").append(password);
    url = sb.toString();

    master = new PsqlInstanceConfigBuilder()
        .withDb(db)
        .withHost(host)
        .withPort(port)
        .withUser(user)
        .withPassword(password)
        .withReadOnly(readOnly)
        .build();
  }
}
