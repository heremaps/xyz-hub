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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.Nullable;

/**
 * A PostgresQL database configuration as used by the {@link PsqlStorage}.
 */
@AvailableSince(NakshaVersion.v2_0_0)
public class PsqlStorageProperties extends XyzProperties {

  @AvailableSince(NakshaVersion.v2_0_0)
  public static final String DB_CONFIG = "dbConfig";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String URL = "url";

  /**
   * The configuration of the PostgresQL database.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  @JsonProperty(DB_CONFIG)
  private @Nullable PsqlConfig dbConfig;

  /**
   * The JDBC URL of the PostgresQL database, for example
   * <pre>{@code jdbc:postgresql://HOST/DB?user=USER&password=PASSWORD&schema=SCHEMA}</pre>. Beware that when the {@code url} is provided,
   * the {@link #dbConfig} is generated from it, but only if no {@link #dbConfig} is given (so being {@code null} or {@code undefined}).
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  @JsonProperty(URL)
  private @Nullable String url;

  /**
   * Create new PostgresQL storage configuration properties.
   *
   * @param dbConfig the database configuration to use.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  @JsonCreator
  protected PsqlStorageProperties(@JsonProperty(DB_CONFIG) PsqlConfig dbConfig, @JsonProperty(URL) String url) {
    if (url != null && dbConfig == null) {
      dbConfig = new PsqlConfigBuilder().parseUrl(url).build();
    }
    this.dbConfig = dbConfig;
  }

  /**
   * Create new PostgresQL storage configuration properties.
   *
   * @param dbConfig the database configuration to use.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  public PsqlStorageProperties(@Nullable PsqlConfig dbConfig) {
    this(dbConfig, null);
  }

  /**
   * Create new PostgresQL storage configuration properties.
   *
   * @param jdbcUrl the JDBC URL of the configuration to use.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public PsqlStorageProperties(@Nullable String jdbcUrl) {
    this(null, jdbcUrl);
  }

  public @Nullable PsqlConfig getDbConfig() {
    return dbConfig;
  }

  public void setDbConfig(@Nullable PsqlConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  @AvailableSince(NakshaVersion.v2_0_7)
  public @Nullable String getUrl() {
    return url;
  }

  public void setUrl(@Nullable String url) {
    this.url = url;
  }
}
