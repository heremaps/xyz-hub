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

  public static NakshaHubConfig readConfigFile(final @NotNull String configId, final @NotNull String appName)
      throws IOException {
    NakshaHubConfig cfg = null;
    try (final Json json = Json.get()) {
      final IoHelp.LoadedBytes loaded = IoHelp.readBytesFromHomeOrResource(configId + ".json", false, appName);
      cfg = json.reader(ViewDeserialize.Storage.class)
          .forType(NakshaHubConfig.class)
          .readValue(loaded.bytes());
      logger.info("Fetched supplied server config from {}", loaded.path());
    }
    return cfg;
  }

  public static NakshaHubConfig readConfigFile(final @NotNull String configId) throws IOException {
    return readConfigFile(configId, "naksha");
  }
}
