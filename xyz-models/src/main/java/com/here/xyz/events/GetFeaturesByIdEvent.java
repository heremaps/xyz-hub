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
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "GetFeaturesByIdEvent")
@SuppressWarnings({"WeakerAccess", "unused"})
public final class GetFeaturesByIdEvent extends Event<GetFeaturesByIdEvent> {

  private List<String> ids;
  private List<String> selection;
  private boolean force2D;

  public List<String> getIds() {
    return this.ids;
  }

  public void setIds(List<String> ids) {
    this.ids = ids;
  }

  public GetFeaturesByIdEvent withIds(List<String> ids) {
    setIds(ids);
    return this;
  }

  public List<String> getSelection() {
    return this.selection;
  }

  public void setSelection(List<String> selection) {
    this.selection = selection;
  }

  public GetFeaturesByIdEvent withSelection(List<String> selection) {
    setSelection(selection);
    return this;
  }

  public boolean isForce2D() {
    return force2D;
  }

  @SuppressWarnings("WeakerAccess")
  public void setForce2D(boolean force2D) {
    this.force2D = force2D;
  }

  @SuppressWarnings("unused")
  public GetFeaturesByIdEvent withForce2D(boolean force2D) {
    setForce2D(force2D);
    return this;
  }
}
