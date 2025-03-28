/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.steps.resources.Load.toLoadsMap;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.config.JobConfigClient;
import com.here.xyz.jobs.steps.execution.db.Database;
import io.vertx.core.Future;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ResourcesRegistry {

  private static final Logger logger = LogManager.getLogger();
  /**
   * Provides the overall number of virtual units being currently reserved for all resources in the system.
   *
   * @return A map of all resource reservations
   */
  protected static final Future<Map<ExecutionResource, Double>> getReservedVirtualUnits() {
    return JobConfigClient.getInstance()
        .loadJobs(RUNNING)
        .compose(runningJobs -> Future.all(runningJobs.stream()
            .map(job -> {
              try {
                return job.calculateResourceLoads().recover(t -> handleResourceLoadError(job, t));
              }
              catch (Exception e) {
                return handleResourceLoadError(job, e);
              }
            }).toList()))
        .map(cf -> cf.<List<Load>>list().stream().flatMap(List::stream).toList())
        .map(loads -> toLoadsMap(loads, false));
  }

  private static Future<List<Load>> handleResourceLoadError(Job job, Throwable t) {
    logger.error("Error calculating the reserved resources of job {}. Not taking it into account this time.", job.getId(), t);
    return Future.succeededFuture(List.of());
  }

  /**
   * Provides the number of virtual units which are not in use, that are still available to be used by incoming jobs.
   *
   * @return The number of virtual units that are still free to be used
   */
  public static Future<Map<ExecutionResource, Double>> getFreeVirtualUnits() {
    return getAllResources()
        .compose(allResources -> getReservedVirtualUnits()
            //Calculate the free virtual units of all resources being currently utilized ...
            .map(reservedVirtualUnits -> reservedVirtualUnits.keySet().stream().collect(Collectors.toMap(Function.identity(),
                resource -> resource.getMaxVirtualUnits() - (reservedVirtualUnits.containsKey(resource)
                    ? reservedVirtualUnits.get(resource) : 0))))
            //Add the maximum value for all resources currently not being utilized
            .map(freeVirtualUnitsOfUtilizedResources -> {
              Map<ExecutionResource, Double> freeVirtualUnits = new HashMap<>(freeVirtualUnitsOfUtilizedResources);
              allResources.stream()
                  .filter(resource -> !freeVirtualUnits.containsKey(resource))
                  .forEach(resource -> freeVirtualUnits.put(resource, resource.getMaxVirtualUnits()));
              return freeVirtualUnits;
            }));
  }

  /**
   * Provides all resource instances that are available in the system.
   * Step implementations can use this method to gather a resource and reserve a load on it inside its "needed resources".
   *
   * @return A list of all available execution resources in the system
   */
  private static Future<List<ExecutionResource>> getAllResources() {
    return Database.getAll()
        .map(databases -> {
          List<ExecutionResource> resources = new ArrayList<>();
          resources.addAll(databases);
          resources.add(IOResource.getInstance());
          return resources;
        });
  }
}
