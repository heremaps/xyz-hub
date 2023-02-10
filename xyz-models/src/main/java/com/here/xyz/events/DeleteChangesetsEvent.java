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
  private long minVersion;
  private Long minTag;

  public long getMinVersion() {
    return minVersion;
  }

  public void setMinVersion(long minVersion) {
    this.minVersion = minVersion;
  }

  public DeleteChangesetsEvent withMinVersion(long minVersion) {
    setMinVersion(minVersion);
    return this;
  }

  public Long getMinTag() {
    return minTag;
  }

  public void setMinTag(Long minTag) {
    this.minTag = minTag;
  }

  public DeleteChangesetsEvent withMinTag(Long minTag) {
    setMinTag(minTag);
    return this;
  }
}
