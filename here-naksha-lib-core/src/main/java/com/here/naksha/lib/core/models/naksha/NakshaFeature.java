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
package com.here.naksha.lib.core.models.naksha;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.Nullable;

/**
 * The base class of all features managed by the Naksha-Hub.
 */
@JsonTypeName(value = "NakshaFeature")
@JsonSubTypes({@JsonSubTypes.Type(value = XyzCollection.class)
  // TODO: Rename to NakshaEventTarget and to NakshaSpace, NakshaSubscription, ...
  // ,@JsonSubTypes.Type(value = EventTarget.class)
})
public class NakshaFeature extends XyzFeature {

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String TITLE = "title";

  @AvailableSince(NakshaVersion.v2_0_7)
  public static final String DESCRIPTION = "description";

  /**
   * Create a new empty feature.
   *
   * @param id The ID; if {@code null}, then a random one is generated.
   */
  @JsonCreator
  public NakshaFeature(@JsonProperty(ID) @Nullable String id) {
    super(id);
  }

  public @Nullable String getTitle() {
    return title;
  }

  public void setTitle(@Nullable String title) {
    this.title = title;
  }

  public @Nullable String getDescription() {
    return description;
  }

  public void setDescription(@Nullable String description) {
    this.description = description;
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(TITLE)
  @JsonInclude(Include.NON_EMPTY)
  private @Nullable String title;

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(DESCRIPTION)
  @JsonInclude(Include.NON_EMPTY)
  private @Nullable String description;
}
