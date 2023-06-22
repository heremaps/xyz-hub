/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.naksha.lib.core.models.payload.events.feature.history;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.payload.events.feature.SearchForFeaturesEvent;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "IterateHistoryEvent")
public final class IterateHistoryEvent extends SearchForFeaturesEvent {

  private String pageToken;

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private int startVersion;

  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private int endVersion;

  private boolean compact;

  public int getStartVersion() {
    return startVersion;
  }

  public void setStartVersion(int startVersion) {
    this.startVersion = startVersion;
  }

  public IterateHistoryEvent withStartVersion(int startVersion) {
    setStartVersion(startVersion);
    return this;
  }

  public int getEndVersion() {
    return endVersion;
  }

  public void setEndVersion(int endVersion) {
    this.endVersion = endVersion;
  }

  public IterateHistoryEvent withEndVersion(int endVersion) {
    setEndVersion(endVersion);
    return this;
  }

  @SuppressWarnings("unused")
  public String getPageToken() {
    return pageToken;
  }

  @SuppressWarnings("WeakerAccess")
  public void setPageToken(String pageToken) {
    this.pageToken = pageToken;
  }

  @SuppressWarnings("unused")
  public IterateHistoryEvent withPageToken(String pageToken) {
    setPageToken(pageToken);
    return this;
  }

  public boolean isCompact() {
    return compact;
  }

  public void setCompact(Boolean compact) {
    this.compact = compact;
  }

  public IterateHistoryEvent withCompact(boolean compact) {
    setCompact(compact);
    return this;
  }
}
