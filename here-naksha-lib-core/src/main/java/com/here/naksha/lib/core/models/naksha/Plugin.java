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
package com.here.naksha.lib.core.models.naksha;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class Plugin<API, SELF extends Plugin<API, SELF>> extends NakshaFeature {

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String CLASS_NAME = "className";

  /**
   * Create a new empty feature.
   *
   * @param className the name of the class to instantiate.
   * @param id The ID; if {@code null}, then a random one is generated.
   */
  protected Plugin(@JsonProperty(CLASS_NAME) @NotNull String className, @JsonProperty(ID) @Nullable String id) {
    super(id);
    this.className = className;
  }

  public @NotNull String getClassName() {
    return className;
  }

  public void setClassName(@NotNull String className) {
    this.className = className;
  }

  @JsonProperty(CLASS_NAME)
  @JsonInclude(Include.ALWAYS)
  protected @NotNull String className;

  /**
   * Create a new instance of the plugin.
   *
   * @param naksha the reference to the Naksha-Hub that wants to have the instance.
   * @return the API implementing the plugin.
   */
  public abstract @NotNull API newInstance(@NotNull INaksha naksha);
}
