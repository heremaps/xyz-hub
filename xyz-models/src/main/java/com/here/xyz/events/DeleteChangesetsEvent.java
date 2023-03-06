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

package com.here.xyz.events;

public class DeleteChangesetsEvent extends Event<DeleteChangesetsEvent> {
  private long requestedMinVersion;
  private Long minTagVersion;

  public long getRequestedMinVersion() {
    return requestedMinVersion;
  }

  public void setRequestedMinVersion(long requestedMinVersion) {
    this.requestedMinVersion = requestedMinVersion;
  }

  public DeleteChangesetsEvent withRequestedMinVersion(long minVersion) {
    setRequestedMinVersion(minVersion);
    return this;
  }

  public Long getMinTagVersion() {
    return minTagVersion;
  }

  public void setMinTagVersion(Long minTagVersion) {
    this.minTagVersion = minTagVersion;
  }

  public DeleteChangesetsEvent withMinTagVersion(Long minTag) {
    setMinTagVersion(minTag);
    return this;
  }
}
