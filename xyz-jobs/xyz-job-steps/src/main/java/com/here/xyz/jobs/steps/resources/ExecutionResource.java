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

package com.here.xyz.jobs.steps.resources;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.vertx.core.Future;

public abstract class ExecutionResource {

  /**
   * Retrieves and provides the utilized actual units of this resource from the underlying resource.
   *
   * @return The number of units being actually utilized on the underlying resource
   */
  public abstract Future<Double> getUtilizedUnits();

  /**
   * Retrieves and provides the available actual units of this resource from the underlying resource.
   *
   * @return The number of units being actually available on the underlying resource
   */
  public Future<Double> getAvailableUnits() {
    return getUtilizedUnits().compose(utilizedUnits -> Future.succeededFuture(getMaxUnits() - utilizedUnits));
  }

  /**
   * Provides the overall maximum value of actual units on this resource from the underlying resource.
   *
   * @return The maximum of the actual units on the underlying resource
   */
  protected abstract double getMaxUnits();

  /**
   * Provides the overall virtual units of this resource being available to the system in general.
   *
   * @return The overall available virtual units of this resource
   */
  protected abstract double getMaxVirtualUnits();

  @JsonIgnore
  protected abstract String getId();

  @Override
  public int hashCode() {
    return getId().hashCode();
  }

  @Override
  public boolean equals(Object obj) {
    return obj instanceof ExecutionResource resource && getId().equals(resource.getId());
  }
}
