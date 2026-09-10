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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;

public class StepGraphReferencesTest {

  @Test
  public void referencingThenDereferencingRestoresTheOriginalGraph() {
    Map<String, Object> stepGraph = sampleStepGraph();
    Map<String, Object> expected = sampleStepGraph();
    Map<String, Map<String, Object>> stepsById = collectSteps(sampleStepGraph());

    assertFalse(StepGraphReferences.containsRef(stepGraph), "an inline graph has no refs");
    Object referenced = StepGraphReferences.reference(stepGraph);
    assertTrue(StepGraphReferences.containsRef(referenced), "after referencing the graph has refs");
    assertEquals(expected, stepGraph, "reference must not mutate its input");

    Object dereferenced = StepGraphReferences.dereference(referenced, stepsById);
    assertEquals(expected, dereferenced, "reference -> dereference must round-trip to the original graph");
  }

  @Test
  public void nestedParallelStructureIsPreserved() {
    Object referenced = StepGraphReferences.reference(sampleStepGraph());

    List<?> executions = (List<?>) ((Map<?, ?>) referenced).get("executions");
    assertEquals(Map.of("$ref", "s_qivcvc"), executions.get(0), "the first step becomes a $ref");

    Map<?, ?> subGraph = (Map<?, ?>) executions.get(1);
    assertEquals(true, subGraph.get("parallel"), "the nested sub-graph's parallel flag is preserved");
    List<?> subExecutions = (List<?>) subGraph.get("executions");
    assertEquals(Map.of("$ref", "s_oeooov"), subExecutions.get(0));
    assertEquals(Map.of("$ref", "s_mpoksk"), subExecutions.get(1));
  }

  @Test
  public void inlineOldStyleGraphIsNotTreatedAsReferenced() {
    assertFalse(StepGraphReferences.containsRef(sampleStepGraph()));
  }

  @Test
  public void dereferenceThrowsWhenAReferencedStepIsMissing() {
    Object referenced = StepGraphReferences.reference(sampleStepGraph());
    assertThrows(IllegalStateException.class, () -> StepGraphReferences.dereference(referenced, Map.of()));
  }

  @Test
  public void concurrentRoundTripsDoNotInterfere() throws Exception {
    final int threads = 16;
    final int iterationsPerThread = 250;
    final Map<String, Object> expected = sampleStepGraph();

    ExecutorService pool = Executors.newFixedThreadPool(threads);
    try {
      List<Future<?>> futures = new ArrayList<>();
      for (int t = 0; t < threads; t++) {
        futures.add(pool.submit(() -> {
          for (int i = 0; i < iterationsPerThread; i++) {
            Map<String, Map<String, Object>> stepsById = collectSteps(sampleStepGraph());
            Object referenced = StepGraphReferences.reference(sampleStepGraph());
            Object dereferenced = StepGraphReferences.dereference(referenced, stepsById);
            assertEquals(expected, dereferenced);
          }
        }));
      }
      for (Future<?> future : futures)
        future.get(); //propagates any assertion failure from the worker threads
    }
    finally {
      pool.shutdownNow();
    }
  }

  // A serialized step graph mirroring a real export job
  private static Map<String, Object> sampleStepGraph() {
    return stepGraph(false, List.of(
        step("s_qivcvc", "ExportSpaceToFiles"),
        stepGraph(true, List.of(
            step("s_oeooov", "CompressFiles"),
            step("s_mpoksk", "S3MetricsCollectorStep")))));
  }

  private static Map<String, Object> stepGraph(boolean parallel, List<Map<String, Object>> executions) {
    Map<String, Object> stepGraph = new HashMap<>();
    stepGraph.put("type", "StepGraph");
    stepGraph.put("parallel", parallel);
    stepGraph.put("executions", new ArrayList<Object>(executions));
    return stepGraph;
  }

  private static Map<String, Object> step(String id, String type) {
    Map<String, Object> step = new HashMap<>();
    step.put("id", id);
    step.put("type", type);
    step.put("jobId", "j1");
    step.put("status", new HashMap<>(Map.of("state", "SUCCEEDED")));
    return step;
  }

  private static Map<String, Map<String, Object>> collectSteps(Object node) {
    Map<String, Map<String, Object>> byId = new HashMap<>();
    collect(node, byId);
    return byId;
  }

  @SuppressWarnings("unchecked")
  private static void collect(Object node, Map<String, Map<String, Object>> byId) {
    if (!(node instanceof Map<?, ?> stepGraph) || !(stepGraph.get("executions") instanceof List<?> executions))
      return;
    for (Object execution : executions)
      if (execution instanceof Map<?, ?> map) {
        if (map.containsKey("executions"))
          collect(map, byId);
        else if (map.get("id") != null)
          byId.put((String) map.get("id"), (Map<String, Object>) map);
      }
  }
}
