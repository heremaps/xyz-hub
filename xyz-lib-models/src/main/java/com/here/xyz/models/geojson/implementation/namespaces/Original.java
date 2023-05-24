/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.models.geojson.implementation.namespaces;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.JsonObject;
import com.here.xyz.view.View;
import org.jetbrains.annotations.Nullable;

/**
 * The standard properties of the standard feature store in the Naksha-Hub.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Original extends JsonObject {

  public static final String CREATED_AT = "createdAt";
  public static final String PUUID = "puuid";
  public static final String MUUID = "muuid";
  public static final String SPACE = "space";
  public static final String UPDATED_AT = "updatedAt";

  /**
   * The space ID the feature belongs to.
   */
  @JsonProperty(SPACE)
  private String space;

  /**
   * The timestamp, when a feature was created.
   */
  @JsonProperty(CREATED_AT)
  private long createdAt;

  /**
   * The timestamp, when a feature was last updated.
   */
  @JsonProperty(UPDATED_AT)
  private long updatedAt;

  /**
   * The previous uuid of the feature.
   */
  @JsonProperty(PUUID)
  @JsonInclude(Include.NON_NULL)
  private String puuid;

  /**
   * The merge muuid of the feature.
   */
  @JsonProperty(MUUID)
  @JsonInclude(Include.NON_NULL)
  private String muuid;

  public @Nullable String getSpace() {
    return space;
  }

  public void setSpace(@Nullable String space) {
    this.space = space;
  }

  public long getCreatedAt() {
    return createdAt;
  }

  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  public long getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
  }

  public @Nullable String getPuuid() {
    return puuid;
  }

  public void setPuuid(@Nullable String puuid) {
    this.puuid = puuid;
  }

  public @Nullable String getMuuid() {
    return muuid;
  }

  public void setMuuid(@Nullable String muuid) {
    this.muuid = muuid;
  }
}

