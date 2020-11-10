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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Check the status of the storage connector.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "HealthCheckEvent")
public final class HealthCheckEvent extends Event<HealthCheckEvent> {

  private long minResponseTime = 0L;
  private int warmupCount = 0;

  /**
   * Returns the minimal response time in milliseconds. The storage connector should not send a response until at least the given amount of
   * milliseconds have passed.
   *
   * @return the minimal amount of milliseconds to wait.
   */
  public long getMinResponseTime() {
    return minResponseTime;
  }

  public void setMinResponseTime(long minResponseTime) {
    this.minResponseTime = minResponseTime;
  }

  public HealthCheckEvent withMinResponseTime(long timeInMilliseconds) {
    setMinResponseTime(timeInMilliseconds);
    return this;
  }

  /**
   * Returns the amount of runtime-environments which should be kept "warmed-up" in order to react
   * quickly to incoming traffic.
   * The implementing connector should ensure to keep that amount of runtime-environments ready.
   *
   * @return the amount of runtime-environments to keep ready.
   */
  public int getWarmupCount() {
    return warmupCount;
  }

  public void setWarmupCount(int warmupCount) {
    this.warmupCount = warmupCount;
  }

  public HealthCheckEvent withWarmupCount(int warmupCount) {
    setWarmupCount(warmupCount);
    return this;
  }
}
