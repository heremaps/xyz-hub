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
package com.here.naksha.lib.core.models.payload.responses.changesets;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.payload.XyzResponse;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Changeset")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class Changeset extends XyzResponse {

  @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
  private XyzFeatureCollection inserted;

  @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
  private XyzFeatureCollection updated;

  @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
  private XyzFeatureCollection deleted;

  public XyzFeatureCollection getInserted() {
    return inserted;
  }

  public void setInserted(XyzFeatureCollection inserted) {
    this.inserted = inserted;
  }

  public Changeset withInserted(final XyzFeatureCollection inserted) {
    setInserted(inserted);
    return this;
  }

  public XyzFeatureCollection getUpdated() {
    return updated;
  }

  public void setUpdated(XyzFeatureCollection updated) {
    this.updated = updated;
  }

  public Changeset withUpdated(final XyzFeatureCollection updated) {
    setUpdated(updated);
    return this;
  }

  public XyzFeatureCollection getDeleted() {
    return deleted;
  }

  public void setDeleted(XyzFeatureCollection deleted) {
    this.deleted = deleted;
  }

  public Changeset withDeleted(final XyzFeatureCollection deleted) {
    setDeleted(deleted);
    return this;
  }
}
