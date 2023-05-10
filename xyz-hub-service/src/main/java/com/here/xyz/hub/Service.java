/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class Service extends NakshaHub {

  /**
   * Create a new XYZ-Hub service instance using the given configuration.
   *
   * @param config The configuration to use.
   * @throws IOException If loading the build properties failed.
   */
  public Service(@NotNull NakshaHubConfig config) throws IOException {
    super(config);
  }

  /**
   * The service instance.
   */
  public static Service instance;

  /**
   * The service entry point.
   */
  public static void main(final @NotNull String @NotNull [] arguments) throws IOException {
    final List<@NotNull String> args = Arrays.asList(arguments);
    final NakshaHubConfig config;
    final int configIndex = args.indexOf("--config");
    if (configIndex >= 0) {
      final String[] split = args.get(configIndex).split("=", 1);
      if (split.length != 2 || split[1].length() == 0) {
        System.err.println("Invalid --config parameter, syntax: --config={path}");
        System.exit(1);
        return;
      }
      config = new NakshaHubConfig(split[1]);
    } else {
      config = new NakshaHubConfig();
    }
    if (args.contains("--debug")) {
      config.debug = true;
    }
    if (config.loadPath() != null) {
      System.out.println("Loaded configuration file: " + config.loadPath());
    }
    instance = new Service(config);
    instance.start();
  }
}