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

package com.here.xyz.responses.changesets;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.responses.XyzResponse;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "ChangesetCollection")
@JsonInclude(JsonInclude.Include.NON_EMPTY)
public class ChangesetCollection extends XyzResponse<ChangesetCollection> {
  private long startVersion;
  private long endVersion;

  @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
  @JsonInclude(JsonInclude.Include.ALWAYS)
  private Map<Long, Changeset> versions;

  private String nextPageToken;

  @SuppressWarnings("unused")
  public String getNextPageToken() {
    return nextPageToken;
  }

  @SuppressWarnings("WeakerAccess")
  public void setNextPageToken(String nextPageToken) {
    this.nextPageToken = nextPageToken;
  }

  public ChangesetCollection withNextPageToken(final String nextPageToken) {
    setNextPageToken(nextPageToken);
    return this;
  }
  public long getStartVersion() {
    return startVersion;
  }

  public void setStartVersion(long startVersion) {
    this.startVersion = startVersion;
  }

  public ChangesetCollection withStartVersion(long startVersion) {
    setStartVersion(startVersion);
    return this;
  }

  public long getEndVersion() {
    return endVersion;
  }

  public void setEndVersion(long endVersion) {
    this.endVersion = endVersion;
  }

  public ChangesetCollection withEndVersion(long withEndVersion) {
    setEndVersion(withEndVersion);
    return this;
  }

  public Map<Long, Changeset> getVersions() {
    return versions;
  }

  public void setVersions(Map<Long, Changeset> versions) {
    this.versions = versions;
  }

  public ChangesetCollection withVersions(final Map<Long, Changeset> versions) {
    setVersions(versions);
    return this;
  }
}
