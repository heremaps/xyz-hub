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

package com.here.xyz.jobs.steps;

import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.HashSet;
import java.util.Set;

/**
 * Only used during compilation to set all common step params
 */
@JsonTypeName(value = "StepGraph")
public class CompilationStepGraph extends StepGraph {
  public CompilationStepGraph() {}

  /**
   * This method should be called after the whole step graph was created to set the needed properties on the steps:
   * - job ID
   * - previous step IDs
   * - Starting state: NOT_READY
   *
   * @param jobId The job ID of the job for which this step graph was created
   * @return This StepGraph for chaining
   */
  public StepGraph enrich(String jobId) {
    enrich(this, jobId);
    return this;
  }

  /**
   * This method should be called after the whole step graph was created to set the needed properties on the steps:
   * - job ID
   * - previous step IDs
   * - Starting state: NOT_READY
   *
   * @param stepGraph The step graph to be enriched with the jobId
   * @param jobId The job ID of the job for which this step graph was created
   */
  public static void enrich(StepGraph stepGraph, String jobId) {
    enrich(stepGraph, jobId, new HashSet<>());
  }

  /**
   * Will be called internally to recursively define the needed properties on the steps of this graph.
   *
   * @param jobId The job ID of the job for which this step graph was created
   * @param previousSteps The steps that should be treated as the previous steps for the first step(s) of this graph
   * @return The steps that should be treated as previous steps for the next sequential execution (sibling of this graph)
   *  in a potential parent graph
   */
  private static Set<String> enrich(StepGraph stepGraph, String jobId, Set<String> previousSteps) {
    HashSet<String> previousStepsForNextSibling = new HashSet<>();

    for (StepExecution child : stepGraph.getExecutions()) {
      if (!stepGraph.isParallel())
        previousStepsForNextSibling = new HashSet<>();

      previousStepsForNextSibling.addAll(enrichChild(child, jobId, previousSteps));

      if (!stepGraph.isParallel())
        previousSteps = previousStepsForNextSibling;
    }

    return previousStepsForNextSibling;
  }

  private static Set<String> enrichChild(StepExecution child, String jobId, Set<String> previousSteps) {
    return child instanceof Step step ? enrichChild(step, jobId, previousSteps)
        : enrich(((StepGraph) child), jobId, previousSteps);
  }

  private static Set<String> enrichChild(Step step, String jobId, Set<String> previousSteps) {
    step.withJobId(jobId)
        .withPreviousStepIds(previousSteps);
    return Set.of(step.getId());
  }
}
