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
package com.here.naksha.lib.core;
/**
 * The DefaultRequestLimitManager class is an implementation of the IRequestLimitManager interface
 * providing default behavior for retrieving request limits.
 */
public class DefaultRequestLimitManager implements IRequestLimitManager {
  private final long instanceLevelLimit;
  private final double actorLimitPct;

  /**
   * Retrieves the number of available processors.
   *
   * @return The number of available processors.
   */
  private static long getAvailableProcessors() {
    return Runtime.getRuntime().availableProcessors();
  }

  /**
   * Constructs a DefaultRequestLimitManager instance with default values.
   * The instance-level limit is calculated based on the available processors.
   * This function is useful where Hub is not involved
   */
  public DefaultRequestLimitManager() {
    this.instanceLevelLimit = 30L * getAvailableProcessors();
    this.actorLimitPct = 25; // 25%
  }

  /**
   * Constructs a DefaultRequestLimitManager instance with custom values.
   *
   * @param cpuLevelLimit   The limit per CPU level.
   * @param actorLimitPct   The percentage of actor limit.
   */
  public DefaultRequestLimitManager(int cpuLevelLimit, int actorLimitPct) {
    this.instanceLevelLimit = cpuLevelLimit * getAvailableProcessors();
    this.actorLimitPct = actorLimitPct;
  }

  /**
   * Retrieves the instance-level request limit.
   *
   * @return The instance-level request limit.
   */
  @Override
  public long getInstanceLevelLimit() {
    return instanceLevelLimit;
  }

  /**
   * Retrieves the request limit for a specific actor within the given context.
   * The actor-level limit is calculated as a percentage of the instance-level limit.
   *
   * @param context The NakshaContext representing the context in which the actor operates.
   * @return The request limit for the actor within the given context.
   */
  @Override
  public long getActorLevelLimit(NakshaContext context) {
    return (long) ((instanceLevelLimit * actorLimitPct) / 100);
  }
}
