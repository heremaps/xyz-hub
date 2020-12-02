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
@JsonTypeName(value = "IterateFeaturesEvent")
public final class IterateFeaturesEvent extends SearchForFeaturesEvent<IterateFeaturesEvent> {

  private String handle;
  private Integer v;

  public Integer getV() {
    return v;
  }

  public void setV(Integer v) {
    this.v = v;
  }

  public IterateFeaturesEvent withV(Integer v) {
    setV(v);
    return this;
  }

  @SuppressWarnings("unused")
  public String getHandle() {
    return handle;
  }

  @SuppressWarnings("WeakerAccess")
  public void setHandle(String handle) {
    this.handle = handle;
  }

  @SuppressWarnings("unused")
  public IterateFeaturesEvent withHandle(String handle) {
    setHandle(handle);
    return this;
  }
}
