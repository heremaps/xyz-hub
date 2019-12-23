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
import com.here.xyz.models.geojson.coordinates.BBox;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "GetFeaturesByBBoxEvent")
public class GetFeaturesByBBoxEvent<T extends GetFeaturesByBBoxEvent> extends SpatialQueryEvent<T> {

  private boolean clip;
  private BBox bbox;

  @SuppressWarnings("unused")
  public Boolean getClip() {
    return this.clip;
  }

  @SuppressWarnings("WeakerAccess")
  public void setClip(Boolean clip) {
    this.clip = clip;
  }

  @SuppressWarnings("unused")
  public T withClip(boolean clip) {
    setClip(clip);
    //noinspection unchecked
    return (T) this;
  }

  public BBox getBbox() {
    return this.bbox;
  }

  public void setBbox(BBox bbox) {
    this.bbox = bbox;
  }

  @SuppressWarnings("unused")
  public T withBbox(BBox bbox) {
    setBbox(bbox);
    //noinspection unchecked
    return (T) this;
  }
}
