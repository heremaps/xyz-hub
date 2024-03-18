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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

@JsonSubTypes({
    @JsonSubTypes.Type(value = CompilationStepGraph.class)
})
public class StepGraph implements StepExecution {
  private List<StepExecution> executions = new LinkedList<>();
  boolean parallel;

  public Stream<Step> stepStream() {
    return executions.stream()
        .flatMap(execution -> execution instanceof StepGraph subGraph ? subGraph.stepStream()
            : Collections.singletonList((Step) execution).stream());
  }

  public List<StepExecution> getExecutions() {
    return executions;
  }

  public void setExecutions(List<StepExecution> executions) {
    this.executions = executions;
  }

  public StepGraph withExecutions(List<StepExecution> executions) {
    setExecutions(executions);
    return this;
  }

  public StepGraph addExecution(StepExecution execution) {
    executions.add(execution);
    return this;
  }

  public void replaceStep(Step<?> step) {
    //TODO: Find and replace the step in this graph, throw an exception if the step was not found
  }

  public boolean isParallel() {
    return parallel;
  }

  public void setParallel(boolean parallel) {
    this.parallel = parallel;
  }

  public StepGraph withParallel(boolean parallel) {
    setParallel(parallel);
    return this;
  }
}
