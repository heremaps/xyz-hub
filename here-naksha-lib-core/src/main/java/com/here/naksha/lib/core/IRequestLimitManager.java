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
 * The IRequestLimitManager interface defines methods for retrieving request limits
 * at different levels - instance level and actor level.
 */
public interface IRequestLimitManager {
  /**
   * Retrieves the instance-level request limit.
   *
   * @return The instance-level request limit.
   */
  long getInstanceLevelLimit();
  /**
   * Retrieves the request limit for a specific actor within the given context.
   *
   * @param context The NakshaContext representing the context in which the actor operates.
   * @return The request limit for the actor within the given context.
   */
  long getActorLevelLimit(NakshaContext context);
}
