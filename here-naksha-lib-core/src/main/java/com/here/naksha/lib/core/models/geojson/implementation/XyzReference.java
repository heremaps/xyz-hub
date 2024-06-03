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
package com.here.naksha.lib.core.models.geojson.implementation;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.util.json.JsonObject;
import com.here.naksha.lib.core.util.json.JsonSerializable;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Xyz reference object holding minimum equivalent fields from MOM reference object
 */
@AvailableSince(NakshaVersion.v2_0_11)
public class XyzReference extends JsonObject {

  @AvailableSince(NakshaVersion.v2_0_11)
  public static final String ID = "id";

  @JsonProperty(ID)
  @JsonInclude(Include.NON_NULL)
  private @Nullable String id;

  @AvailableSince(NakshaVersion.v2_0_11)
  public static final String SPACE_ID = "spaceId";

  @JsonProperty(SPACE_ID)
  @JsonInclude(Include.NON_NULL)
  private @Nullable String spaceId;

  @AvailableSince(NakshaVersion.v2_0_11)
  public static final String FEATURE_TYPE = "featureType";

  @JsonProperty(FEATURE_TYPE)
  @JsonInclude(Include.NON_NULL)
  private @Nullable String featureType;

  /**
   * Create a new empty reference
   */
  public XyzReference() {}

  /**
   * Create a new empty reference
   */
  public XyzReference(final @Nullable String id, final @Nullable String spaceId, final @Nullable String featureType) {
    this.id = id;
    this.spaceId = spaceId;
    this.featureType = featureType;
  }

  @AvailableSince(NakshaVersion.v2_0_11)
  public @Nullable String getId() {
    return id;
  }

  @AvailableSince(NakshaVersion.v2_0_11)
  public void setId(final @Nullable String id) {
    this.id = id;
  }

  @AvailableSince(NakshaVersion.v2_0_11)
  public @Nullable String getSpaceId() {
    return spaceId;
  }

  @AvailableSince(NakshaVersion.v2_0_11)
  public void setSpaceId(final @Nullable String spaceId) {
    this.spaceId = spaceId;
  }

  @AvailableSince(NakshaVersion.v2_0_11)
  public @Nullable String getFeatureType() {
    return featureType;
  }

  @AvailableSince(NakshaVersion.v2_0_11)
  public void setFeatureType(final @Nullable String featureType) {
    this.featureType = featureType;
  }

  @Override
  public @NotNull String toString() {
    return JsonSerializable.toString(this);
  }
}
