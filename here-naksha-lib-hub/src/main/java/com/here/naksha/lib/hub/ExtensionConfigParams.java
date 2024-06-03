/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.lib.hub;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import java.util.List;
import org.jetbrains.annotations.Nullable;

@JsonTypeName
public class ExtensionConfigParams implements JsonSerializable {
  public static final String WHITELIST_CLASSES = "whitelistClasses";
  public static final String INTERVAL_MS = "intervalms";
  public static final String EXTENSION_ROOT_PATH = "extensionsRootPath";

  @JsonProperty(WHITELIST_CLASSES)
  List<String> whiteListClasses;

  @JsonProperty(INTERVAL_MS)
  long intervalMs;

  @JsonProperty(EXTENSION_ROOT_PATH)
  String extensionRootPath;
  /**
   * Create an extension.
   *
   * @param whiteListClasses  List of whitelist urls used in classloader
   * @param intervalMs config expiry in millisecond
   * @param extensionRootPath extensions root directory
   */
  @JsonCreator
  public ExtensionConfigParams(
      @JsonProperty(WHITELIST_CLASSES) @Nullable List<String> whiteListClasses,
      @JsonProperty(INTERVAL_MS) @Nullable Long intervalMs,
      @JsonProperty(EXTENSION_ROOT_PATH) @Nullable String extensionRootPath) {
    this.whiteListClasses =
        whiteListClasses == null ? List.of("java.*", "javax.*", "com.here.naksha.*") : whiteListClasses;
    this.intervalMs = (intervalMs == null ? 300000 : intervalMs);
    this.extensionRootPath = extensionRootPath;
  }

  public List<String> getWhiteListClasses() {
    return whiteListClasses;
  }

  public long getIntervalMs() {
    return intervalMs;
  }

  public String getExtensionRootPath() {
    return extensionRootPath;
  }
}
