/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.naksha.lib.core.models.payload.events.feature;

import static com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileResponseType.GEO_JSON;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.jetbrains.annotations.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "GetFeaturesByTileEvent")
@SuppressWarnings("unused")
public final class GetFeaturesByTileEvent extends GetFeaturesByBBoxEvent {

  private int level;
  private int x;
  private int y;
  private String quadkey;
  private int margin;
  private @NotNull GetFeaturesByTileResponseType responseType = GEO_JSON;
  private boolean hereTileFlag;

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

  public GetFeaturesByTileEvent withLevel(int level) {
    setLevel(level);
    return this;
  }

  public int getX() {
    return x;
  }

  public void setX(int x) {
    this.x = x;
  }

  public GetFeaturesByTileEvent withX(int x) {
    setX(x);
    return this;
  }

  public int getY() {
    return y;
  }

  public void setY(int y) {
    this.y = y;
  }

  public GetFeaturesByTileEvent withY(int y) {
    setY(y);
    return this;
  }

  public String getQuadkey() {
    return quadkey;
  }

  public void setQuadkey(String quadkey) {
    this.quadkey = quadkey;
  }

  public GetFeaturesByTileEvent withQuadkey(String quadkey) {
    setQuadkey(quadkey);
    return this;
  }

  public boolean getHereTileFlag() {
    return hereTileFlag;
  }

  public void setHereTileFlag(boolean hereTileFlag) {
    this.hereTileFlag = hereTileFlag;
  }

  public GetFeaturesByTileEvent withHereTileFlag(boolean hereTileFlag) {
    setHereTileFlag(hereTileFlag);
    return this;
  }

  public int getMargin() {
    return margin;
  }

  public void setMargin(int margin) {
    this.margin = margin;
  }

  public GetFeaturesByTileEvent withMargin(int margin) {
    setMargin(margin);
    return this;
  }

  public @NotNull GetFeaturesByTileResponseType getResponseType() {
    return responseType;
  }

  public void setResponseType(@NotNull GetFeaturesByTileResponseType responseType) {
    this.responseType = responseType;
  }

  public GetFeaturesByTileEvent withResponseType(GetFeaturesByTileResponseType responseType) {
    setResponseType(responseType);
    return this;
  }
}
