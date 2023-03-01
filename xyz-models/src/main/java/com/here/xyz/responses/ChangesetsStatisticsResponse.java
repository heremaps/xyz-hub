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

package com.here.xyz.responses;

import com.fasterxml.jackson.annotation.JsonInclude;

public class ChangesetsStatisticsResponse extends XyzResponse<ChangesetsStatisticsResponse> {
  private Long minVersion;
  private Long maxVersion;

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private Long minTagVersion;

  public Long getMinVersion() {
    return minVersion;
  }

  public void setMinVersion(Long  minVersion) {
    this.minVersion = minVersion;
  }

  public ChangesetsStatisticsResponse withMinVersion(Long minVersion) {
    setMinVersion(minVersion);
    return this;
  }

  public Long getMaxVersion() {
    return maxVersion;
  }

  public void setMaxVersion(Long  maxVersion) {
    this.maxVersion = maxVersion;
  }

  public ChangesetsStatisticsResponse withMaxVersion(Long maxVersion) {
    setMaxVersion(maxVersion);
    return this;
  }

  public Long getMinTagVersion() {
    return minTagVersion;
  }

  public void setTagMinVersion(Long minTagVersion) {
    this.minTagVersion = minTagVersion;
  }

  public ChangesetsStatisticsResponse withTagVersion(Long minTagVersion) {
    setTagMinVersion(minTagVersion);
    return this;
  }
}