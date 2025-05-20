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

package com.here.xyz.jobs.steps.execution;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.resources.ExecutionResource;
import com.here.xyz.jobs.steps.resources.ResourcesRegistry;
import io.vertx.core.Future;
import java.util.LinkedList;
import java.util.Map;

public class GraphSequencingTool {

  public static Future<Void> optimize(Job job) {
    Future<Map<ExecutionResource, Double>>  freeVirtualUnits = ResourcesRegistry.getFreeVirtualUnits();
    job.setExecutionSteps(copyStepGraph(job.getSteps()));
    return optimize(freeVirtualUnits, job);
  }

  protected static Future<Void> optimize(Future<Map<ExecutionResource, Double>>  freeVirtualUnits, Job job) {
    return JobExecutor.getInstance().mayExecute(freeVirtualUnits, job)
            .compose(mayExecute -> {
              if(mayExecute || !hasParallelExecutions(job.getExecutionSteps()))
                return Future.succeededFuture();

              partiallySequenceGraph(job.getExecutionSteps());
              return optimize(freeVirtualUnits, job);
            });
  }

  /**
   * @param stepGraph
   * @return Returns 'true' if the graph (or sub-graph) has any parallel executions
   */
  private static boolean hasParallelExecutions(StepGraph stepGraph) {
    if(stepGraph.isParallel()) return true;

    for(StepExecution execution : stepGraph.getExecutions()) {
      if(execution instanceof StepGraph subStepGraph && hasParallelExecutions(subStepGraph))
        return true;
    }

    return false;
  }

  /**
   * Sequentialize first parallel graph (or sub-graph), after splitting into two parallel sub-graphs <br>
   * Examples:
   * <ul>
   *   <li>parallel(A, B, C, D) -> sequential(parallel(A,B), parallel(C,D))</li>
   *   <li>sequential(A, parallel(B, c, D)) -> sequential(A, sequential(parallel(B, C), parallel(D))))</li>
   * </ul>
   *
   */
  private static void partiallySequenceGraph(StepGraph stepGraph) {
    if (stepGraph.isParallel()) {
      int executionSize = stepGraph.getExecutions().size();
      if (executionSize <= 2)
        stepGraph.withParallel(false);
      else {
        //TODO: Find a way to split the steps with same ExecutionResource into different sub-graphs
        StepGraph splitGraph1 = new StepGraph()
                .withExecutions(stepGraph.getExecutions().subList(0, executionSize/2))
                .withParallel(true);
        StepGraph splitGraph2 = new StepGraph()
                .withExecutions(stepGraph.getExecutions().subList(executionSize/2, executionSize))
                .withParallel(true);
        stepGraph.withExecutions(new LinkedList<>())
                .addExecution(splitGraph1)
                .addExecution(splitGraph2)
                .withParallel(false);
      }
    } else {
      for (StepExecution execution : stepGraph.getExecutions()) {
        if (execution instanceof StepGraph subStepGraph && hasParallelExecutions(subStepGraph)) {
          partiallySequenceGraph(subStepGraph);
          return;
        }
      }
    }
  }

  private static StepGraph copyStepGraph(StepGraph stepGraph) {
    StepGraph copyStepGraph = new StepGraph().withParallel(stepGraph.isParallel());
    for(StepExecution execution : stepGraph.getExecutions()) {
      if(execution instanceof StepGraph subStepGraph)
        copyStepGraph.addExecution(copyStepGraph(subStepGraph));
      else
        copyStepGraph.addExecution(execution);
    }
    return copyStepGraph;
  }

}
