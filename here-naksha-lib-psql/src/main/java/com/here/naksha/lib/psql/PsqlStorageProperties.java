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
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** A PostgresQL database configuration as used by the {@link PsqlStorage}. */
@AvailableSince(NakshaVersion.v2_0_0)
public class PsqlStorageProperties extends XyzProperties {

  @AvailableSince(NakshaVersion.v2_0_0)
  public static final String CONFIG = "config";

  /**
   * Create new PostgresQL storage configuration properties.
   *
   * @param config the database configuration to use.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  @JsonCreator
  public PsqlStorageProperties(@NotNull PsqlConfig config) {
    this.config = config;
  }

  /** The configuration of the PostgresQL database. */
  @AvailableSince(NakshaVersion.v2_0_0)
  public @NotNull PsqlConfig config;
}
