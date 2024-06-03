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
package com.here.naksha.lib.core.models.features;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An extension is an administrative feature that allows to run proprietary code, outside the Naksha-Hub using proprietary libraries.
 */
@AvailableSince(NakshaVersion.v2_0_3)
@JsonTypeName
public class Extension extends XyzFeature {
  public static final String URL = "url";
  public static final String VERSION = "version";
  public static final String INIT_CLASS_NAME = "initClassName";

  @JsonProperty(URL)
  String url;

  @JsonProperty(VERSION)
  String version;

  @JsonProperty(INIT_CLASS_NAME)
  String initClassName;

  /**
   * Create an extension.
   *
   * @param id  Unique identifier of extension.
   * @param url source url of given extension.
   * @param version version of extension.
   * @param initClassName Extension initialisation class.
   */
  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonCreator
  public Extension(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(URL) @NotNull String url,
      @JsonProperty(VERSION) @NotNull String version,
      @JsonProperty(INIT_CLASS_NAME) @Nullable String initClassName) {
    super(id);
    this.url = url;
    this.version = version;
    this.initClassName = initClassName;
  }

  public String getUrl() {
    return url;
  }

  public String getVersion() {
    return version;
  }

  public String getInitClassName() {
    return initClassName;
  }
}
