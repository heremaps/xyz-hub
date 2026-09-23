/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Helper Class to reference / de-reference the step configs. A referenced step is a placeholder {@code {"$ref": "<stepId>"}}.
 */
public final class StepGraphReferences {

  private static final String REF_KEY = "$ref";

  private StepGraphReferences() {
  }

  /**
   * Returns a copy of the given step graph in which each step node is replaced with a {@code {"$ref": stepId}} placeholder,
   */
  @SuppressWarnings("unchecked")
  public static Object reference(Object steps) {
    if (isSubGraph(steps)) {
      Map<String, Object> stepGraph = (Map<String, Object>) steps;
      List<Object> referencedExecutions = new ArrayList<>();
      for (Object execution : (List<Object>) stepGraph.get("executions")) {
        referencedExecutions.add(reference(execution));
      }
      return withExecutions(stepGraph, referencedExecutions);
    }
    if (steps instanceof Map<?, ?> stepMap && stepMap.get("id") != null) {
      return new HashMap<>(Map.of(REF_KEY, stepMap.get("id")));
    }
    return steps;
  }

  /**
   * Returns a copy of the given step graph in which each {@code {"$ref": stepId}} placeholder is replaced with the step config map from
   * {@code stepsById}, preserving the topology. The input is not modified; anything that is not a step graph is returned unchanged.
   *
   * @throws IllegalStateException if a referenced step is missing from {@code stepsById}.
   */
  @SuppressWarnings("unchecked")
  static Object dereference(Object steps, Map<String, Map<String, Object>> stepsById) {
    if (steps instanceof Map<?, ?> node && node.get(REF_KEY) != null) {
      Object ref = node.get(REF_KEY);
      Map<String, Object> stepConfig = stepsById.get((String) ref);
      if (stepConfig == null) {
        throw new IllegalStateException("Missing step config for referenced step '" + ref + "'");
      }
      return stepConfig;
    }
    if (isSubGraph(steps)) {
      Map<String, Object> stepGraph = (Map<String, Object>) steps;
      List<Object> dereferencedExecutions = new ArrayList<>();
      for (Object execution : (List<Object>) stepGraph.get("executions")) {
        dereferencedExecutions.add(dereference(execution, stepsById));
      }
      return withExecutions(stepGraph, dereferencedExecutions);
    }
    return steps;
  }

  /**
   * Returns a shallow copy of the step graph node with its {@code "executions"} replaced.
   */
  private static Map<String, Object> withExecutions(Map<String, Object> stepGraph, List<Object> executions) {
    Map<String, Object> copy = new HashMap<>(stepGraph);
    copy.put("executions", executions);
    return copy;
  }

  /**
   * Whether the serialized step graph contains any {@code {"$ref": ...}} placeholder, i.e. it is a new-style (split) graph. Returns
   * {@code false} for old-style items whose step configs are still inlined - those are returned as-is without any step-table read.
   */
  static boolean containsRef(Object steps) {
    if (!(steps instanceof Map<?, ?> stepGraphs)) {
      return false;
    }
    if (stepGraphs.get(REF_KEY) != null) {
      return true;
    }
    return stepGraphs.get("executions") instanceof List<?> executions
        && executions.stream().anyMatch(StepGraphReferences::containsRef);
  }

  private static boolean isSubGraph(Object node) {
    return node instanceof Map<?, ?> map && map.containsKey("executions");
  }
}
