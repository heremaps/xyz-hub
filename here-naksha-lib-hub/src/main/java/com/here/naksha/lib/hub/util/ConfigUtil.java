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
package com.here.naksha.lib.hub.util;

import com.here.naksha.lib.core.util.IoHelp;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.view.ViewDeserialize;
import com.here.naksha.lib.hub.NakshaHubConfig;
import java.io.IOException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtil {

  private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);

  public static final String DEF_CFG_PATH_ENV = "NAKSHA_CONFIG_PATH";

  private ConfigUtil() {}

  public static NakshaHubConfig readConfigFile(final @NotNull String configId, final @NotNull String appName)
      throws IOException {
    NakshaHubConfig cfg = null;
    try (final Json json = Json.get()) {
      // use the path provided in NAKSHA_CONFIG_PATH (if it is set)
      final String envVal = System.getenv(DEF_CFG_PATH_ENV);
      final String path = envVal == null || envVal.isEmpty() || "null".equalsIgnoreCase(envVal) ? null : envVal;
      // attempt loading config from file
      final IoHelp.LoadedBytes loaded =
          IoHelp.readBytesFromHomeOrResource(configId + ".json", false, appName, path);
      cfg = json.reader(ViewDeserialize.Storage.class)
          .forType(NakshaHubConfig.class)
          .readValue(loaded.getBytes());
      logger.info("Fetched supplied server config from {}", loaded.getPath());
    }
    return cfg;
  }

  public static NakshaHubConfig readConfigFile(final @NotNull String configId) throws IOException {
    return readConfigFile(configId, "naksha");
  }
}
