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

package com.here.xyz.jobs.steps.execution;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.Step.InputSet;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.StepGraph;
import java.util.ArrayList;
import java.util.List;

public class GraphFusionTool {

  /**
   * Tries to fuse a provided {@link StepGraph} with the one of the provided Job.
   * "Fusing" here refers to replace the steps in the job's graph by the ones of the old graph
   * that are equivalent (in regard to the {@link Step#isEquivalentTo(StepExecution)} check).
   *
   * @param newJob The new job that contains a compiled StepGraph into which to fuse the old graph's steps
   * @param oldGraph The old graph from which to re-use the step executions and their outputs
   * @return A graph that is equivalent to the new job's graph but contains as many replaced steps from the old graph as possible.
   */
  protected static StepGraph fuseGraphs(Job newJob, StepGraph oldGraph) {
    return fuseGraphs(newJob.getId(), newJob.getSteps(), oldGraph);
  }

  protected static StepGraph fuseGraphs(String newJobId, StepGraph newGraph, StepGraph oldGraph) {
    CompilationStepGraph fusedGraph = replaceByDelegations(newGraph, oldGraph);

    //Replace previous step relations (previousStepIds)
    fusedGraph.enrich(newJobId);

    //Replace InputSets accordingly for new steps that should re-use outputs of old steps as inputs
    resolveReusedInputs(fusedGraph);
    return fusedGraph;
  }

  /**
   * Replaces all steps of the new step graph that can be re-used from the old step graph by a {@link DelegateStep} that
   * points to the according equivalent step of the old step graph being re-used.
   *
   * @param newStepGraph The new step graph to use as template to generate a new graph with replacements
   * @param oldStepGraph The old step graph of which to take the steps that can be re-used
   * @return A graph that is equivalent to the new step graph, but contains as many DelegateSteps as possible
   */
  private static CompilationStepGraph replaceByDelegations(StepGraph newStepGraph, StepGraph oldStepGraph) {
    //TODO: Handle different parallelities correctly? Also what about stepgraphs of size 1
    if (newStepGraph.isParallel() != oldStepGraph.isParallel())
      return null;
    return newStepGraph.isParallel()
        ? replaceByDelegationsParallelly(newStepGraph, oldStepGraph) : replaceByDelegationsSequentially(newStepGraph, oldStepGraph);
  }

  /**
   * Performs the replacements for sequential graphs only.
   * @see #replaceByDelegations(StepGraph, StepGraph)
   *
   * @param newStepGraph
   * @param oldStepGraph
   * @return
   */
  private static CompilationStepGraph replaceByDelegationsSequentially(StepGraph newStepGraph, StepGraph oldStepGraph) {
    CompilationStepGraph result = new CompilationStepGraph();
    //TODO: Take into account the length of the step graphs
    for (int i = 0; i < Math.min(newStepGraph.getExecutions().size(), oldStepGraph.getExecutions().size()); i++) {
      StepExecution newExecution = newStepGraph.getExecutions().get(i);
      StepExecution oldExecution = oldStepGraph.getExecutions().get(i);

      if (newExecution instanceof StepGraph newSubGraph && oldExecution instanceof StepGraph oldSubGraph)
        result.addExecution(replaceByDelegations(newSubGraph, oldSubGraph));
      else if (oldExecution.isEquivalentTo(newExecution))
        //Replace the new execution by a pseudo delegating step that has a reference to the old step
        result.addExecution(delegateToOldStep((Step) newExecution, (Step) oldExecution));
      else {
        //Keep all further (remaining) new executions as they are
        result.getExecutions().addAll(newStepGraph.getExecutions().subList(i, newStepGraph.getExecutions().size()));
        break;
      }
    }
    return result;
  }

  /**
   * Performs the replacements for parallel graphs only.
   * @see #replaceByDelegations(StepGraph, StepGraph)
   *
   * @param newStepGraph
   * @param oldStepGraph
   * @return
   */
  private static CompilationStepGraph replaceByDelegationsParallelly(StepGraph newStepGraph, StepGraph oldStepGraph) {
    CompilationStepGraph result = (CompilationStepGraph) new CompilationStepGraph().withParallel(true);
    for (StepExecution newBranch : newStepGraph.getExecutions()) {
      //Each execution is one parallel branch. Calculate the match count with each old parallel branch
      int maxMatchCount = 0;
      StepExecution largestMatchingBranch = null;
      for (StepExecution oldBranch : oldStepGraph.getExecutions()) {
        StepExecution branchCandidate = replaceByDelegationsAsFarAsPossible(newBranch, oldBranch);
        int matchCount = matchCount(branchCandidate);
        if (matchCount > maxMatchCount) {
          maxMatchCount = matchCount;
          //A better match was found
          largestMatchingBranch = branchCandidate;
        }
      }

      if (largestMatchingBranch == null)
        //No match was found for the new branch, so fully add it to the resulting graph
        result.addExecution(newBranch);
      else
        result.addExecution(largestMatchingBranch);
    }
    return result;
  }

  /**
   * Performs the replacements for one parallel branch. (That could be one step or a graph)
   * @see #replaceByDelegations(StepGraph, StepGraph)
   *
   * @param newBranch
   * @param oldBranch
   * @return
   */
  private static StepExecution replaceByDelegationsAsFarAsPossible(StepExecution newBranch, StepExecution oldBranch) {
    if (newBranch instanceof StepGraph newBranchGraph && oldBranch instanceof StepGraph oldBranchGraph) {
      return replaceByDelegations(newBranchGraph, oldBranchGraph);
    }
    else if (newBranch instanceof Step newStep && oldBranch instanceof Step oldStep) {
      return newStep.isEquivalentTo(oldStep) ? delegateToOldStep(newStep, oldStep) : newStep;
    }
    //TODO: Compare also graphs with only one step to one step
    else
      return newBranch;
  }

  /**
   * Creates a {@link DelegateStep} to be used to replace a step from the new graph with one equivalent step from the old graph.
   *
   * @param newStep
   * @param oldStep
   * @return
   */
  private static DelegateStep delegateToOldStep(Step newStep, Step oldStep) {
    return new DelegateStep(oldStep, newStep);
  }

  /**
   * Counts the number of steps that have been replaced by {@link DelegateStep}s in the provided parallel branch.
   *
   * @param branch
   * @return
   */
  private static int matchCount(StepExecution branch) {
    return branch instanceof StepGraph graph ? (int) graph.stepStream().filter(step -> step instanceof DelegateStep).count()
        : branch instanceof DelegateStep ? 1 : 0;
  }

  /**
   * Resolves all output-to-input assignments of the graph that is currently being fused by re-weaving the re-used outputs with the
   * consuming steps.
   * That is being done by overwriting the {@link InputSet}s of the consuming steps with the new references of the according
   * {@link DelegateStep}s.
   * Should be only called for a graph **after** performing all delegation-replacements.
   *
   * @param graph
   */
  static void resolveReusedInputs(StepGraph graph) {
    graph.stepStream().forEach(step -> resolveReusedInputs(step, graph));
  }

  /**
   * Resolves the inputSets of a new step that is potentially consuming outputs of an old step referenced by a {@link DelegateStep}.
   *
   * @param step
   * @param containingStepGraph
   */
  private static void resolveReusedInputs(Step step, StepGraph containingStepGraph) {
    List<InputSet> newInputSets = new ArrayList<>();
    for (InputSet compiledInputSet : (List<InputSet>) step.getInputSets()) {
      if (compiledInputSet.stepId() == null || !(containingStepGraph.getStep(compiledInputSet.stepId()) instanceof DelegateStep replacementStep))
        //NOTE: stepId == null on an InputSet refers to the USER-inputs
        newInputSets.add(compiledInputSet);
      else
        //Now we know that inputSet is one that should be replaced by one that is pointing to the outputs of the old graph
        newInputSets.add(new InputSet(replacementStep.getDelegate().getJobId(), replacementStep.getDelegate().getId(), compiledInputSet.name()));
    }
    step.setInputSets(newInputSets);
  }
}
