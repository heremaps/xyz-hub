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

package com.here.xyz.jobs.steps;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.List;

/**
 * Only used during compilation to set all common step params
 */
class CompilationStepGraph extends StepGraph {
  private String jobId;

  private CompilationStepGraph() {}

  CompilationStepGraph(String jobId) {
    this.jobId = jobId;
  }

  @Override
  public void setExecutions(List<StepExecution> executions) {
    executions.forEach(execution -> enrichStep(execution));
    super.setExecutions(executions);
  }

  @Override
  public StepGraph addExecution(StepExecution execution) {
    enrichStep(execution);
    return super.addExecution(execution);
  }

  private void enrichStep(StepExecution execution) {
    if (execution instanceof Step step)
      step.withJobId(jobId).withPreviousStepId(getPreviousStepId());
  }

  @JsonIgnore
  private String getPreviousStepId() {
    List<StepExecution> executions = getExecutions();
    if (!executions.isEmpty()) {
      final StepExecution stepExecution = executions.get(executions.size() - 1);
      if (stepExecution instanceof Step previousStep)
        return previousStep.getId();
      else
        //TODO: Proper solution for parallel step-graphs
        return ((CompilationStepGraph) stepExecution).getPreviousStepId();
    }
    return null;
  }
}
