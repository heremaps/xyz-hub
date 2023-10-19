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
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/** A Connector properties holding database configuration as used by the {@link EventHandler}. */
@AvailableSince(NakshaVersion.v2_0_6)
public class ConnectorProperties extends XyzProperties {

  /** The configuration of the PostgresQL database. */
  @AvailableSince(NakshaVersion.v2_0_6)
  public PsqlConfig dbConfig;

  /** A list of {@link PsqlConfig}'s to be used as read-replicas (if available), for read operations. */
  @AvailableSince(NakshaVersion.v2_0_6)
  public List<PsqlConfig> dbReplicas;

  public PsqlConfig getDbConfig() {
    return dbConfig;
  }

  public void setDbConfig(@NotNull PsqlConfig dbConfig) {
    this.dbConfig = dbConfig;
  }

  public List<PsqlConfig> getDbReplicas() {
    return dbReplicas;
  }

  public void setDbReplicas(@NotNull List<PsqlConfig> dbReplicas) {
    this.dbReplicas = dbReplicas;
  }
}
