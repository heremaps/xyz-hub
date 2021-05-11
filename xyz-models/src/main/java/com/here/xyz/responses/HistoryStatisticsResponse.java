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

package com.here.xyz.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.events.GetStatisticsEvent;

/**
 * The response that is sent for a {@link GetStatisticsEvent}.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "HistoryStatisticsResponse")
public class HistoryStatisticsResponse extends XyzResponse<HistoryStatisticsResponse> {

  private StatisticsResponse.Value<Long> count;
  @Deprecated
  private StatisticsResponse.Value<Long>  byteSize;
  private StatisticsResponse.Value<Long>  dataSize;
  private StatisticsResponse.Value<Integer>  maxVersion;

  /**
   * Returns the amount of features stored in the space.
   *
   * @return the amount of features stored in the space.
   */
  @SuppressWarnings("unused")
  public StatisticsResponse.Value<Long>  getCount() {
    return this.count;
  }

  /**
   * Sets the amount of features stored in the space.
   *
   * @param count the amount of features stored in the space.
   */
  @SuppressWarnings({"unused", "WeakerAccess"})
  public void setCount(StatisticsResponse.Value<Long>  count) {
    this.count = count;
  }

  /**
   * Sets the amount of features stored in the space.
   *
   * @return this.
   */
  @SuppressWarnings("unused")
  public HistoryStatisticsResponse withCount(StatisticsResponse.Value<Long>  count) {
    setCount(count);
    return this;
  }

  /**
   * Returns the amount of bytes that are stored in the space.
   *
   * @return the amount of bytes that are stored in the space.
   */
  @SuppressWarnings({"unused"})
  public StatisticsResponse.Value<Long>  getByteSize() {
    return this.byteSize;
  }

  /**
   * Sets the amount of bytes that are stored in the space.
   *
   * @param byteSize the amount of bytes that are stored in the space.
   */
  @SuppressWarnings({"WeakerAccess"})
  public void setByteSize(StatisticsResponse.Value<Long> byteSize) {
    this.byteSize = byteSize;
  }

  /**
   * Sets the amount of bytes that are stored in the space.
   *
   * @return this.
   */
  @SuppressWarnings({"unused"})
  public HistoryStatisticsResponse withByteSize(StatisticsResponse.Value<Long>  byteSize) {
    setByteSize(byteSize);
    return this;
  }

  /**
   * Returns the amount of bytes that are stored in the space.
   *
   * @return the amount of bytes that are stored in the space.
   */
  @SuppressWarnings({"unused"})
  public StatisticsResponse.Value<Long>  getDataSize() {
    return this.dataSize;
  }

  /**
   * Sets the amount of bytes that are stored in the space.
   *
   * @param dataSize the amount of bytes that are stored in the space.
   */
  @SuppressWarnings({"WeakerAccess"})
  public void setDataSize(StatisticsResponse.Value<Long> dataSize) {
    this.dataSize = dataSize;
  }

  /**
   * Sets the amount of bytes that are stored in the space.
   *
   * @return this.
   */
  @SuppressWarnings({"unused"})
  public HistoryStatisticsResponse withDataSize(StatisticsResponse.Value<Long>  dataSize) {
    setDataSize(dataSize);
    return this;
  }

  public StatisticsResponse.Value<Integer>  getMaxVersion() {
    return maxVersion;
  }

  public void setMaxVersion(StatisticsResponse.Value<Integer>  maxVersion) {
    this.maxVersion = maxVersion;
  }

  public HistoryStatisticsResponse withMaxVersion(StatisticsResponse.Value<Integer> maxVersion) {
    setMaxVersion(maxVersion);
    return this;
  }

}
