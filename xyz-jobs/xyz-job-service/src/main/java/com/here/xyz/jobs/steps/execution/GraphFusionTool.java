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

import static com.here.xyz.jobs.steps.execution.RunEmrJob.toInputSetReference;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.Step.InputSet;
import com.here.xyz.jobs.steps.Step.OutputSet;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.RunEmrJob.ReferenceIdentifier;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

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
    newGraph = canonicalize(newGraph);
    oldGraph = canonicalize(oldGraph);
    CompilationStepGraph fusedGraph = replaceByDelegations(newGraph, oldGraph);

    //Replace previous step relations (previousStepIds)
    fusedGraph.enrich(newJobId);

    //Replace InputSets accordingly for new steps that should re-use outputs of old steps as inputs
    resolveReusedInputs(fusedGraph);
    return fusedGraph;
  }

  protected static StepGraph canonicalize(StepGraph graph) {
    /*
    1.) Remove all steps that are flagged as being "notReusable" (these should be basically hidden from the reusability process)
    2.) Then, remove empty sub-graphs (NOTE: The traversal is done in "bottom-up" manner so sub-graphs
      that became empty due to the removal of "notReusable" steps will be removed as well
     */
    traverse(graph, execution -> {
      if (execution instanceof Step step && step.isNotReusable())
        return null;
      if (execution instanceof StepGraph subGraph) {
        if (subGraph.isEmpty())
          return null;
        if (subGraph.getExecutions().size() == 1)
          return unwrap(subGraph);
      }
      return execution;
    });
    //Unwrap the top-level graph if it only contains one subgraph
    if (graph.getExecutions().size() == 1 && graph.getExecutions().get(0) instanceof StepGraph subGraph)
      graph = subGraph;

    return graph;
  }

  /**
   * Traverses all executions of the specified graph in a bottom-up manner (leaves first).
   * @param graph The graph to be traversed recursively
   * @param processor The action to be performed on the execution-node and its containing graph
   */
  private static void traverse(StepGraph graph, UnaryOperator<StepExecution> processor) {
    List<StepExecution> nodes = new LinkedList<>(graph.getExecutions());
    Iterator<StepExecution> nodeIterator = nodes.iterator();
    int index = 0;
    while (nodeIterator.hasNext()) {
      StepExecution execution = nodeIterator.next();
      if (execution instanceof Step step)
        execution = processor.apply(step);
      else if (execution instanceof StepGraph subGraph) {
        //First traverse, then call the processor (==> "bottom-up")
        traverse(subGraph, processor);
        execution = processor.apply(subGraph);
      }

      //Update executions accordingly to the return value of the processor (null means removal)
      if (execution == null) {
        nodeIterator.remove();
        index--;
      }
      else
        nodes.set(index, execution);
      index++;
    }
    graph.setExecutions(nodes);
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
    if (newStepGraph.isParallel() != oldStepGraph.isParallel()) {
      if (newStepGraph.isParallel())
        //Wrap the sequential old graph into a parallel one and continue
        oldStepGraph = wrap(oldStepGraph, true);
      else
        //Wrap the sequential new graph into a parallel one, do replacements parallel, and unwrap the result again
        return (CompilationStepGraph) unwrap(replaceByDelegationsParallelly(wrap(newStepGraph, true), oldStepGraph));
    }
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
    for (int i = 0; i < Math.min(newStepGraph.getExecutions().size(), oldStepGraph.getExecutions().size()); i++) {
      StepExecution newExecution = newStepGraph.getExecutions().get(i);
      StepExecution oldExecution = oldStepGraph.getExecutions().get(i);

      //Replace the new execution by a pseudo delegating step that has a reference to the old step
      //NOTE: Even if one of the executions is a step, it could be that the other one is a StepGraph containing one step
      result.addExecution(replaceByDelegationsInExecution(newExecution, oldExecution));

      if (!oldExecution.isEquivalentTo(newExecution))
        //Keep all further (remaining) new executions of the sequential graph as they are (see below)
        break;
    }
    //In case the new graph was longer than the old graph, add the rest of the steps
    if (result.getExecutions().size() < newStepGraph.getExecutions().size())
      result.getExecutions().addAll(newStepGraph.getExecutions().subList(result.getExecutions().size(), newStepGraph.getExecutions().size()));

    return result;
  }

  private static CompilationStepGraph replaceByDelegation(StepGraph newStepGraph, Step oldStep) {
    if (newStepGraph.isParallel())
      return replaceByDelegationsParallelly(newStepGraph, wrap(oldStep, true));
    else
      return replaceByDelegationsSequentially(newStepGraph, wrap(oldStep, false));
  }

  private static Step replaceByDelegation(Step newStep, StepGraph oldStepGraph) {
    if (oldStepGraph.isParallel())
      return (Step) unwrap(replaceByDelegationsParallelly(wrap(newStep, true), oldStepGraph));
    else
      return (Step) unwrap(replaceByDelegationsSequentially(wrap(newStep, false), oldStepGraph));
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
        StepExecution branchCandidate = replaceByDelegationsInExecution(newBranch, oldBranch);
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
   * Performs the replacements for an execution. (That could be one step or a graph)
   * @see #replaceByDelegations(StepGraph, StepGraph)
   *
   * @param newExecution
   * @param oldExecution
   * @return
   */
  private static StepExecution replaceByDelegationsInExecution(StepExecution newExecution, StepExecution oldExecution) {
    if (newExecution instanceof StepGraph newGraph && oldExecution instanceof StepGraph oldGraph)
      return replaceByDelegations(newGraph, oldGraph);
    else if (newExecution instanceof Step newStep && oldExecution instanceof Step oldStep)
      return newStep.isEquivalentTo(oldStep) ? delegateToOldStep(newStep, oldStep) : newStep;
    else if (newExecution instanceof StepGraph newGraph)
      return replaceByDelegation(newGraph, (Step) oldExecution);
    else if (oldExecution instanceof StepGraph oldGraph)
      return replaceByDelegation((Step) newExecution, oldGraph);
    else
      return newExecution;
  }

  private static StepGraph wrap(StepExecution execution, boolean parallel) {
    return new StepGraph().withParallel(parallel).addExecution(execution);
  }

  /**
   * Unwraps graphs that have only one execution
   * @param oneExecution
   * @return
   */
  private static StepExecution unwrap(StepExecution oneExecution) {
    if (oneExecution instanceof StepGraph graph && graph.getExecutions().size() != 1)
      throw new IllegalArgumentException("Can not unwrap graphs with a number of executions different than 1.");
    return oneExecution instanceof StepGraph singletonStepGraph ? singletonStepGraph.getExecutions().get(0) : oneExecution;
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
  private static void resolveReusedInputs(StepGraph graph) {
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
      if (compiledInputSet.providerId() == null || !(containingStepGraph.getStep(compiledInputSet.providerId()) instanceof DelegateStep replacementStep))
        //NOTE: stepId == null on an InputSet refers to the USER-inputs
        newInputSets.add(compiledInputSet);
      else
        //Now we know that inputSet is one that should be replaced by one that is pointing to the outputs of the old graph
        //Note that stepId in the OutputSet of DelegateStep could be different from the stepId of the DelegateStep
        newInputSets.add(new InputSet(replacementStep.getDelegate().getJobId(),
            replacementStep.getOutputSet(compiledInputSet.name()).getStepId(), compiledInputSet.name(), compiledInputSet.modelBased(),
            replacementStep.getDelegate().getOutputMetadata()));
    }
    step.setInputSets(newInputSets);

    if (step instanceof RunEmrJob emrStep)
      updateEmrScriptParamReferences(emrStep, containingStepGraph);
  }

  private static void updateEmrScriptParamReferences(RunEmrJob runEmrJob, StepGraph containingStepGraph) {
    //... for positional parameters
    runEmrJob.setPositionalScriptParams(runEmrJob.getPositionalScriptParams().stream()
        .map(scriptParam -> runEmrJob.mapInputReferencesIn(scriptParam, referenceIdentifier -> updateEmrScriptParamReferences(runEmrJob, containingStepGraph, referenceIdentifier)))
        .toList());

    //... for named parameters
    runEmrJob.setNamedScriptParams(runEmrJob.getNamedScriptParams().entrySet().stream()
        .collect(Collectors.toMap(namedParam -> namedParam.getKey(),
            namedParam -> runEmrJob.mapInputReferencesIn(namedParam.getValue(), referenceIdentifier -> updateEmrScriptParamReferences(runEmrJob, containingStepGraph, referenceIdentifier)))));
  }

  private static String updateEmrScriptParamReferences(RunEmrJob runEmrJob, StepGraph containingStepGraph, String referenceIdentifier) {
    try {
      //Will throw an exception if the referenced inputSet is not found in the step
      runEmrJob.fromReferenceIdentifier(referenceIdentifier);
      //In case it was found, it means the reference is pointing to a "new" output, keep the reference as it is
      return null;
    }
    catch (IllegalArgumentException e) {
      //If the input was not found, check if the target step is a DelegateStep that contains the input set to be referenced
      ReferenceIdentifier ref = ReferenceIdentifier.fromString(referenceIdentifier);
      Step referencedStep = containingStepGraph.getStep(ref.stepId());
      if (!(referencedStep instanceof DelegateStep referencedDelegateStep))
        /*
        In this case, the reference cannot be dereferenced at all.
        That could only happen if the compiler created an invalid reference.
        Here the reference will be kept as it is, and the later execution will handle this issue by throwing the correct exception.
         */
        return null;
      OutputSet referencedOutputsSet = referencedDelegateStep.getOutputSet(ref.name());
      return toInputSetReference(runEmrJob.getInputSet(referencedOutputsSet.getStepId(), ref.name()));
    }
  }
}
