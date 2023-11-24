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
package com.here.naksha.lib.core.util;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

public class StreamInfo {
  private String spaceId;
  private String storageId;

  public void setSpaceId(final String spaceId) {
    this.spaceId = spaceId;
  }

  public void setStorageId(final String storageId) {
    this.storageId = storageId;
  }

  public void setSpaceIdIfMissing(final String spaceId) {
    if (this.spaceId == null) this.spaceId = spaceId;
  }

  public void setStorageIdIfMissing(final String storageId) {
    if (this.storageId == null) this.storageId = storageId;
  }

  public String getSpaceId() {
    return this.spaceId;
  }

  public String getStorageId() {
    return this.storageId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    StreamInfo that = (StreamInfo) o;
    return Objects.equals(spaceId, that.spaceId) && Objects.equals(storageId, that.storageId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(spaceId, storageId);
  }

  public @NotNull String toColonSeparatedString() {
    return "spaceId=" + ((spaceId == null || spaceId.isEmpty()) ? "-" : spaceId) + ";storageId="
        + ((storageId == null || storageId.isEmpty()) ? "-" : storageId);
  }
}
