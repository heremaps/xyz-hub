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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "GetFeaturesByTileEvent")
public final class GetFeaturesByTileEvent extends GetFeaturesByBBoxEvent<GetFeaturesByTileEvent> {

  private int level;
  private int x;
  private int y;
  private String quadkey;
  private int margin;
  private ResponseType responseType;

  @SuppressWarnings("unused")
  public int getLevel() {
    return level;
  }

  @SuppressWarnings("WeakerAccess")
  public void setLevel(int level) {
    this.level = level;
  }

  @SuppressWarnings("unused")
  public GetFeaturesByTileEvent withLevel(int level) {
    setLevel(level);
    return this;
  }

  @SuppressWarnings("unused")
  public int getX() {
    return x;
  }

  public void setX(int x) {
    this.x = x;
  }

  @SuppressWarnings("unused")
  public GetFeaturesByTileEvent withX(int x) {
    setX(x);
    return this;
  }

  @SuppressWarnings("unused")
  public int getY() {
    return y;
  }

  public void setY(int y) {
    this.y = y;
  }

  @SuppressWarnings("unused")
  public GetFeaturesByTileEvent withY(int y) {
    setY(y);
    return this;
  }

  @SuppressWarnings("unused")
  public String getQuadkey() {
    return quadkey;
  }

  @SuppressWarnings("WeakerAccess")
  public void setQuadkey(String quadkey) {
    this.quadkey = quadkey;
  }

  @SuppressWarnings("unused")
  public GetFeaturesByTileEvent withQuadkey(String quadkey) {
    setQuadkey(quadkey);
    return this;
  }

  @SuppressWarnings("unused")
  public int getMargin() {
    return margin;
  }

  @SuppressWarnings("WeakerAccess")
  public void setMargin(int margin) {
    this.margin = margin;
  }

  @SuppressWarnings("unused")
  public GetFeaturesByTileEvent withMargin(int margin) {
    setMargin(margin);
    return this;
  }

  @SuppressWarnings("unused")
  public ResponseType getResponseType() {
    return responseType;
  }

  @SuppressWarnings("WeakerAccess")
  public void setResponseType(ResponseType responseType) {
    this.responseType = responseType;
  }

  @SuppressWarnings("unused")
  public GetFeaturesByTileEvent withResponseType(ResponseType responseType) {
    setResponseType(responseType);
    return this;
  }

  @SuppressWarnings("unused")
  public enum ResponseType {
    GEOJSON, MVT
  }
}
