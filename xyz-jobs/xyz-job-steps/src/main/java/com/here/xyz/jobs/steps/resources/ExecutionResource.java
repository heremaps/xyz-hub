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

import io.vertx.core.Future;
import java.util.Map;

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
   * Provides the overall number of virtual units being currently reserved for all resources in the system.
   *
   * @return A map of all resource reservations
   */
  protected static final Map<ExecutionResource, Double> getReservedVirtualUnits() {
    //TODO: Make async
    //TODO: Calculate the reservations for this resource for all running jobs
    return null;
  }

  /**
   * Provides the number of virtual units which are not in use and still that are available to be used by incoming jobs.
   *
   * @return The number of virtual units which are still free to be used
   */
  public double getFreeVirtualUnits() {
    //TODO: Make async
    final Map<ExecutionResource, Double> reservedVirtualUnits = getReservedVirtualUnits();
    return getMaxVirtualUnits() - (reservedVirtualUnits.containsKey(this) ? reservedVirtualUnits.get(this) : 0);
  }

  /**
   * Provides the overall virtual units of this resource being available to the system in general.
   *
   * @return The overall available virtual units of this resource
   */
  protected abstract double getMaxVirtualUnits();

  /**
   * Provides all resource instances that are available in the system.
   * Step implementations can use this method to gather a resource and reserve a load on it inside its "needed resources".
   *
   * @return A list of all available execution resources in the system
   */
  //public static List<ExecutionResource> getAllResources() {
  //  List<ExecutionResource> list = new ArrayList<>();
  //  list.addAll(Database.getAll());
  //  return list;
  //}
}
