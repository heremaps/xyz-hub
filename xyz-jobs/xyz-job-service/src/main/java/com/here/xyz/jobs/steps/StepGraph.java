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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.resources.Load;
import com.here.xyz.util.service.BaseHttpServerVerticle.ValidationException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;

@JsonSubTypes({
    @JsonSubTypes.Type(value = CompilationStepGraph.class)
})
@JsonTypeName(value = "StepGraph")
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

  /**
   * Returns a step within this graph matching the provided step ID.
   *
   * @param stepId
   * @return <code>Step</code> if found in the step graph, else return null
   */
  public Step getStep(String stepId) {
    return stepStream()
        .filter(step -> stepId.equals(step.getId()))
        .findFirst()
        .orElse(null);
  }

  /**
   * Replaces a step within this graph by the provided step with the same ID.
   *
   * @param step The step to replace the existing step
   * @return <code>true</code> if a matching step was found in this graph and was actually replaced, <code>false</code> otherwise
   */
  public boolean replaceStep(Step<?> step) {
    for (int i = 0; i < executions.size(); i++) {
      StepExecution execution = executions.get(i);
      if (execution instanceof StepGraph graph) {
        if (graph.replaceStep(step))
          return true;
      }
      else if (execution instanceof Step traversedStep && traversedStep.getId().equals(step.getId())) {
        executions.set(i, step);
        return true;
      }
    }
    return false;
  }

  /**
   * Finds the step by its ID within this graph and returns the full attribute path to it.
   *
   * Sample attribute path:
   *  <code>executions[2].executions[0].executions[0]</code>
   *
   * @param stepId The ID of the step to find within this graph
   * @return The attribute path to the step if it was found, <code>null</code> otherwise.
   */
  public String findStepPath(String stepId) {
    for (int i = 0; i < executions.size(); i++) {
      StepExecution execution = executions.get(i);
      if (execution instanceof StepGraph graph) {
        String stepPath = graph.findStepPath(stepId);
        if (stepPath != null)
          return "executions[" + i + "]." + stepPath;
      }
      else if (execution instanceof Step traversedStep && traversedStep.getId().equals(stepId))
        return "executions[" + i + "]";
    }
    return null;
  }

  public boolean isParallel() {
    return parallel;
  }

  public void setParallel(boolean parallel) {
    this.parallel = Config.instance.PARALLEL_STEPS_SUPPORTED && parallel;
  }

  public StepGraph withParallel(boolean parallel) {
    setParallel(parallel);
    return this;
  }

  /**
   * Returns `true` if all executions of this graph are equivalent to all according executions of the specified graph.
   * @param other
   * @return `true` if all executions of `other` are equivalent with the executions of this graph
   */
  @Override
  public boolean isEquivalentTo(StepExecution other) {
    if (!(other instanceof StepGraph otherGraph)
        || otherGraph.isParallel() != isParallel()
        || executions.size() != otherGraph.executions.size())
      return false;

    //FIXME: If this StepGraph is a parallel one, the order of the executions should not matter. In that case it simply matters, that for each execution there *exists* an equivalent execution in otherGraph
    for (int i = 0; i < executions.size(); i++)
      if (!executions.get(i).isEquivalentTo(otherGraph.executions.get(i)))
        return false;
    return true;
  }

  /**
   * NOTE: The "other" StepGraph is the new one
   * @param other
   * @return
   */
  public StepGraph findConnectedEquivalentSubGraph(StepGraph other) {
    StepExecution currentNode = null;
    StepExecution currentOtherNode = null;
    StepGraph subGraph = new StepGraph()
        .withParallel(other.isParallel());

    if (isEquivalentTo(other))
      return this;
    for (int i = 0; i < executions.size(); i++) {
      currentNode = executions.get(i);
      currentOtherNode = other.executions.get(i);
      if (currentNode instanceof StepGraph currentSubGraph && currentOtherNode instanceof StepGraph currentOtherSubGraph) {
        StepGraph equivalentSubGraph = currentSubGraph.findConnectedEquivalentSubGraph(currentOtherSubGraph);
        if (equivalentSubGraph.isEmpty())
          break;
        subGraph.addExecution(equivalentSubGraph);
      }
      else if (!currentNode.isEquivalentTo(currentOtherNode))
        break;
      else
        subGraph.addExecution(currentNode);
    }

    return subGraph;
  }

  private static StepExecution createPseudoNode(StepExecution previousNode) {
    if (previousNode instanceof StepGraph previousNodeGraph) {
      if (previousNodeGraph.isParallel())
        //Create a pseudo node for each previous node in the preceding parallel graph
        return new StepGraph()
            .withParallel(true)
            .withExecutions(previousNodeGraph.executions.stream().map(prevNode -> createPseudoNode(prevNode)).toList());

      //Get the last node of the sequential previous graph and create a pseudo node for it
      return createPseudoNode(previousNodeGraph.executions.get(previousNodeGraph.executions.size() - 1));
    }

    Step previousStep = (Step) previousNode;
    return new DelegateOutputsPseudoStep(previousStep.getJobId(), previousStep.getId());
  }

  /**
   * Returns `true` if this graph is the empty graph and does not contain any executions.
   * @return `true` if this graph does not contain any executions
   */
  @JsonIgnore
  public boolean isEmpty() {
    return executions.isEmpty();
  }

  /**
   * Returns the full number of steps contained within this StepGraph.
   * @return The number of all steps in this graph
   */
  public int size() {
    return (int) stepStream().count();
  }

  /**
   * NOTE: This step implementation is a placeholder step, that will be used by the JobExecutor to inject outputs of a formerly run
   * job into the StepGraph of this job.
   * This step depicts a step of the formerly run job that was found to be a predecessor of the step at the border of the re-usable subgraph
   * of the formerly run StepGraph that was cut out because it matched a part of the new job's StepGraph.
   * This pseudo step creates a link to the new Job's StepGraph and the old step's outputs.
   * That way the succeeding Step(s) of the new StepGraph can access the outputs that have been produced by the old step.
   */
  private static class DelegateOutputsPseudoStep extends Step<DelegateOutputsPseudoStep> {
    private String delegateJobId; //The old job ID
    private String delegateStepId; //The old step ID

    DelegateOutputsPseudoStep(String delegateJobId, String delegateStepId) {
      this.delegateJobId = delegateJobId;
      this.delegateStepId = delegateStepId;
    }

    @Override
    public List<Load> getNeededResources() {
      return List.of();
    }

    @Override
    public int getTimeoutSeconds() {
      return 0;
    }

    @Override
    public int getEstimatedExecutionSeconds() {
      return 0;
    }

    @Override
    public String getDescription() {
      return "A pseudo step that just delegates outputs of a Step of another StepGraph into this StepGraph";
    }

    @Override
    public void execute() throws Exception {
      throw new IllegalAccessException("This method should never be called");
    }

    @Override
    public void resume() throws Exception {
      execute();
    }

    @Override
    public void cancel() throws Exception {}

    @Override
    public boolean validate() throws ValidationException {
      return true;
    }
  }
}
