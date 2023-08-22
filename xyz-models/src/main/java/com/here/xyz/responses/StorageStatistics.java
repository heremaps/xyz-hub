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

import com.here.xyz.responses.StatisticsResponse.Value;
import java.util.Map;

public class StorageStatistics extends XyzResponse<StorageStatistics> {

  private Map<String, SpaceByteSizes> byteSizes;
  private long createdAt;

  /**
   * @return A map of which the keys are the space IDs and the values are the according byte size information
   * of the according space.
   */
  @SuppressWarnings("unused")
  public Map<String, SpaceByteSizes> getByteSizes() {
    return byteSizes;
  }

  @SuppressWarnings("unused")
  public void setByteSizes(Map<String, SpaceByteSizes> byteSizes) {
    this.byteSizes = byteSizes;
  }

  @SuppressWarnings("unused")
  public StorageStatistics withByteSizes(Map<String, SpaceByteSizes> byteSizes) {
    setByteSizes(byteSizes);
    return this;
  }

  @SuppressWarnings("unused")
  public long getCreatedAt() {
    return createdAt;
  }

  @SuppressWarnings("unused")
  public void setCreatedAt(long createdAt) {
    this.createdAt = createdAt;
  }

  @SuppressWarnings("unused")
  public StorageStatistics withCreatedAt(long createdAt) {
    setCreatedAt(createdAt);
    return this;
  }

  public static class SpaceByteSizes {
    private StatisticsResponse.Value<Long> contentBytes;
    private StatisticsResponse.Value<Long> historyBytes;
    private StatisticsResponse.Value<Long> searchablePropertiesBytes;
    private String error;

    @SuppressWarnings("unused")
    public Value<Long> getContentBytes() {
      return contentBytes;
    }

    @SuppressWarnings("unused")
    public void setContentBytes(Value<Long> contentBytes) {
      this.contentBytes = contentBytes;
    }

    @SuppressWarnings("unused")
    public SpaceByteSizes withContentBytes(Value<Long> contentBytes) {
      setContentBytes(contentBytes);
      return this;
    }

    @SuppressWarnings("unused")
    public Value<Long> getHistoryBytes() {
      return historyBytes;
    }

    @SuppressWarnings("unused")
    public void setHistoryBytes(Value<Long> historyBytes) {
      this.historyBytes = historyBytes;
    }

    @SuppressWarnings("unused")
    public SpaceByteSizes withHistoryBytes(Value<Long> historyBytes) {
      setHistoryBytes(historyBytes);
      return this;
    }

    @SuppressWarnings("unused")
    public Value<Long> getSearchablePropertiesBytes() {
      return searchablePropertiesBytes;
    }

    @SuppressWarnings("unused")
    public void setSearchablePropertiesBytes(Value<Long> searchablePropertiesBytes) {
      this.searchablePropertiesBytes = searchablePropertiesBytes;
    }

    @SuppressWarnings("unused")
    public SpaceByteSizes withSearchablePropertiesBytes(Value<Long> searchablePropertiesBytes) {
      setSearchablePropertiesBytes(searchablePropertiesBytes);
      return this;
    }

    @SuppressWarnings("unused")
    public String getError() {
      return error;
    }

    @SuppressWarnings("unused")
    public void setError(String error) {
      this.error = error;
    }

    @SuppressWarnings("unused")
    public SpaceByteSizes withError(String error) {
      setError(error);
      return this;
    }
  }
}
