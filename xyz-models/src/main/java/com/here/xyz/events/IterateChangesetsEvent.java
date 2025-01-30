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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "IterateChangesetsEvent")
public final class IterateChangesetsEvent extends SearchForFeaturesEvent<IterateChangesetsEvent> {
  private String pageToken;
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long startVersion;
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  private long endVersion = -1;

  private int versionsToKeep;

  public long getStartVersion() {
    return startVersion;
  }

  public void setStartVersion(long startVersion) {
    this.startVersion = startVersion;
  }

  public IterateChangesetsEvent withStartVersion(long startVersion) {
    setStartVersion(startVersion);
    return this;
  }

  public long getEndVersion() {
    return endVersion;
  }

  public void setEndVersion(long endVersion) {
    this.endVersion = endVersion;
  }

  public IterateChangesetsEvent withEndVersion(long endVersion) {
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
  public IterateChangesetsEvent withPageToken(String pageToken) {
    setPageToken(pageToken);
    return this;
  }

  public int getVersionsToKeep() {
    return versionsToKeep;
  }

  public void setVersionsToKeep(int versionsToKeep) {
    this.versionsToKeep = versionsToKeep;
  }

  public IterateChangesetsEvent withVersionsToKeep(int setVersionsToKeep) {
    setVersionsToKeep(versionsToKeep);
    return this;
  }
}
