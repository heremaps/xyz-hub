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
package com.here.naksha.lib.handlers;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzProperties;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.Nullable;

/**
 * Default variant of EventHandler properties supported by Naksha - for TagFilterHandler
 */
@AvailableSince(NakshaVersion.v2_0_13)
public class TagFilterHandlerProperties extends XyzProperties {

  @AvailableSince(NakshaVersion.v2_0_13)
  public static final String ADD_VALUES = "add";

  @AvailableSince(NakshaVersion.v2_0_13)
  public static final String REMOVE_W_PREFIXES = "removeWithPrefixes";

  @AvailableSince(NakshaVersion.v2_0_13)
  public static final String CONTAINS_VALUES = "contains";

  /**
   * To specify list of tags to be added to the {@link XyzFeature} during create/update {@link WriteFeatures} operations.
   */
  @AvailableSince(NakshaVersion.v2_0_13)
  @JsonProperty(ADD_VALUES)
  private @Nullable List<String> add;
  /**
   * To specify prefix-matching tags to be removed from the {@link XyzFeature} during create/update {@link WriteFeatures} operations.
   * This is applied before {@link #add} operation.
   */
  @AvailableSince(NakshaVersion.v2_0_13)
  @JsonProperty(REMOVE_W_PREFIXES)
  private @Nullable List<String> removeWithPrefixes;
  /**
   * To specify list of tags to be added as AND filter condition whenever {@link ReadFeatures} is processed via this handler.
   */
  @AvailableSince(NakshaVersion.v2_0_13)
  @JsonProperty(CONTAINS_VALUES)
  private @Nullable List<String> contains;

  public @Nullable List<String> getAdd() {
    return add;
  }

  public void setAdd(@Nullable final List<String> add) {
    this.add = add;
  }

  public @Nullable List<String> getRemoveWithPrefixes() {
    return removeWithPrefixes;
  }

  public void setRemoveWithPrefixes(final @Nullable List<String> removeWithPrefixes) {
    this.removeWithPrefixes = removeWithPrefixes;
  }

  public @Nullable List<String> getContains() {
    return contains;
  }

  public void setContains(@Nullable List<String> contains) {
    this.contains = contains;
  }
}
