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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Load {

  private ExecutionResource resource;
  private double estimatedVirtualUnits;

  public ExecutionResource getResource() {
    return resource;
  }

  public void setResource(ExecutionResource resource) {
    this.resource = resource;
  }

  public Load withResource(ExecutionResource resource) {
    setResource(resource);
    return this;
  }

  public double getEstimatedVirtualUnits() {
    return estimatedVirtualUnits;
  }

  public void setEstimatedVirtualUnits(double estimatedVirtualUnits) {
    this.estimatedVirtualUnits = estimatedVirtualUnits;
  }

  public Load withEstimatedVirtualUnits(double estimatedVirtualUnits) {
    setEstimatedVirtualUnits(estimatedVirtualUnits);
    return this;
  }

  /*
  Some aggregation helper method
   */
  public static void addLoads(Map<ExecutionResource, Double> loads, Map<ExecutionResource, Double> loadsToAdd, boolean maximize) {
    loadsToAdd.entrySet().forEach(e -> addLoad(loads, e.getKey(), e.getValue(), maximize));
  }

  public static void addLoad(Map<ExecutionResource, Double> loads, Load load, boolean maximize) {
    addLoad(loads, load.getResource(), load.getEstimatedVirtualUnits(), maximize);
  }

  private static void addLoad(Map<ExecutionResource, Double> loads, ExecutionResource resource, double units, boolean maximize) {
    if (maximize)
      loads.put(resource, loads.containsKey(resource) ? Math.max(loads.get(resource), units) : units);
    else
      loads.put(resource, loads.containsKey(resource) ? loads.get(resource) + units : units);
  }

  public static Map<ExecutionResource, Double> toLoadsMap(List<Load> loads, boolean maximize) {
    Map<ExecutionResource, Double> loadsMap = new HashMap<>();
    loads.forEach(load -> addLoad(loadsMap, load.getResource(), load.getEstimatedVirtualUnits(), maximize));
    return loadsMap;
  }
}
