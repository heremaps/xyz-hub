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

import static com.here.xyz.jobs.steps.Step.Visibility.SYSTEM;
import static com.here.xyz.jobs.steps.execution.GraphFusionTool.canonicalize;
import static com.here.xyz.jobs.steps.execution.GraphFusionTool.fuseGraphs;
import static com.here.xyz.jobs.steps.execution.fusion.SimpleTestStepWithOutput.SOME_OUTPUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Internal;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.Step.InputSet;
import com.here.xyz.jobs.steps.Step.OutputSet;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStep;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStepWithOutput;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

public class GraphFusionTests {
  public static final String SOME_EXPORT = "SomeExport";
  public static final String SOME_OTHER_EXPORT = "SomeOtherExport";
  public static final String SOME_CONSUMER = "SomeConsumer";
  public static final String NEW_JOB_ID = "newJob";
  public static final String OLD_JOB_ID = "oldJob";

  private static final boolean printGraphs = false;

  static {
    new Config();
    Config.instance.PARALLEL_STEPS_SUPPORTED = true;
    //FIX
    Config.instance.HUB_ENDPOINT = "http://localhost:8080/hub";
  }

  @CartesianTest
  public void singletonGraphReusable(@Values(booleans = {true, false}) boolean oldIsWrapped, @Values(booleans = {true, false}) boolean oldIsParallel, @Values(booleans = {true, false}) boolean newIsParallel) {
    Step oldProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    StepGraph oldGraph = sequential(OLD_JOB_ID, oldIsWrapped ? createGraph(oldIsParallel, oldProducer) : oldProducer);

    SimpleTestStepWithOutput newProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    StepGraph newGraph = sequential(NEW_JOB_ID, oldIsWrapped ? newProducer : createGraph(newIsParallel, newProducer));

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(1, fusedGraph.size());
    assertTrue(fusedGraph.stepStream().allMatch(step -> step instanceof DelegateStep));
    assertTrue(oldGraph.isEquivalentTo(newGraph));
    assertTrue(fusedGraph.isEquivalentTo(oldGraph));
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @CartesianTest
  public void singletonGraphNotReusable(@Values(booleans = {true, false}) boolean oldIsWrapped, @Values(booleans = {true, false}) boolean oldIsParallel, @Values(booleans = {true, false}) boolean newIsParallel) {
    Step oldProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    StepGraph oldGraph = sequential(OLD_JOB_ID, oldIsWrapped ? createGraph(oldIsParallel, oldProducer) : oldProducer);

    SimpleTestStepWithOutput newProducer = new SimpleTestStepWithOutput(SOME_OTHER_EXPORT);
    StepGraph newGraph = sequential(NEW_JOB_ID, oldIsWrapped ? newProducer : createGraph(newIsParallel, newProducer));

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(1, fusedGraph.size());
    assertTrue(fusedGraph.getExecutions().stream().noneMatch(step -> step instanceof DelegateStep));
    assertFalse(oldGraph.isEquivalentTo(newGraph));
    assertFalse(fusedGraph.isEquivalentTo(oldGraph));
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void reuseSingletonGraphTransitively() {
    Step oldProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    StepGraph oldGraph = sequential(OLD_JOB_ID, oldProducer);

    SimpleTestStepWithOutput newProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    StepGraph newGraph = sequential(NEW_JOB_ID, newProducer);

    StepGraph firstFusedGraph = fuse(newGraph, oldGraph);

    SimpleTestStepWithOutput anotherNewProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    StepGraph anotherNewGraph = sequential(NEW_JOB_ID, newProducer);

    StepGraph fusedGraph = fuse(anotherNewGraph, firstFusedGraph);

    assertEquals(1, fusedGraph.size());
    assertTrue(fusedGraph.stepStream().allMatch(step -> step instanceof DelegateStep delegateStep && delegateStep.getDelegate() instanceof SimpleTestStepWithOutput));
    assertTrue(oldGraph.isEquivalentTo(anotherNewGraph));
    assertTrue(fusedGraph.isEquivalentTo(oldGraph));
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void simpleSequentialGraphFullyReusable() {
    Step oldProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    Step oldConsumer = new SimpleTestStep(SOME_CONSUMER);
    StepGraph oldGraph = sequential(OLD_JOB_ID,
        oldProducer, step(oldConsumer, inputsOf(oldProducer)));

    SimpleTestStepWithOutput newProducer = new SimpleTestStepWithOutput(SOME_EXPORT).withOutputSetVisibility(SOME_OUTPUT, SYSTEM);
    SimpleTestStep newConsumer = new SimpleTestStep(SOME_CONSUMER);
    StepGraph newGraph = sequential(NEW_JOB_ID,
        newProducer, step(newConsumer, inputsOf(newProducer)));

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(2, fusedGraph.getExecutions().size());
    assertTrue(fusedGraph.getExecutions().stream().allMatch(step -> step instanceof DelegateStep));
    assertTrue(oldGraph.isEquivalentTo(newGraph));
    assertTrue(fusedGraph.isEquivalentTo(oldGraph));
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void simpleSequentialGraphPartiallyReusable() {
    SimpleTestStepWithOutput oldProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep oldConsumer = new SimpleTestStep(SOME_CONSUMER);
    StepGraph oldGraph = sequential(OLD_JOB_ID, oldProducer, step(oldConsumer, inputsOf(oldProducer)));

    SimpleTestStepWithOutput newProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep newConsumer = new SimpleTestStep("SomeOtherStep");
    StepGraph newGraph = sequential(NEW_JOB_ID, newProducer, step(newConsumer, inputsOf(newProducer)));

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertTrue(fusedGraph.getExecutions().get(0) instanceof DelegateStep);
    assertTrue(fusedGraph.getExecutions().get(0).isEquivalentTo(oldGraph.getExecutions().get(0)));
    assertTrue(fusedGraph.getExecutions().get(0).isEquivalentTo(newGraph.getExecutions().get(0)));
    assertFalse(fusedGraph.getExecutions().get(1).isEquivalentTo(oldGraph.getExecutions().get(1)));
    assertTrue(fusedGraph.getExecutions().get(1).isEquivalentTo(newGraph.getExecutions().get(1)));
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void simpleSequentialGraphNotReusable() {
    SimpleTestStepWithOutput oldProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep oldConsumer = new SimpleTestStep(SOME_CONSUMER);
    StepGraph oldGraph = sequential(OLD_JOB_ID, oldProducer, step(oldConsumer, inputsOf(oldProducer)));

    SimpleTestStepWithOutput newProducer = new SimpleTestStepWithOutput("SomeOtherExport");
    SimpleTestStep newConsumer = new SimpleTestStep("SomeOtherStep");
    StepGraph newGraph = sequential(NEW_JOB_ID, newProducer, step(newConsumer, inputsOf(newProducer)));

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertTrue(fusedGraph.getExecutions().stream().noneMatch(step -> step instanceof DelegateStep));
    assertFalse(fusedGraph.getExecutions().get(0).isEquivalentTo(oldGraph.getExecutions().get(0)));
    assertFalse(fusedGraph.getExecutions().get(1).isEquivalentTo(oldGraph.getExecutions().get(1)));
    assertTrue(fusedGraph.getExecutions().get(0).isEquivalentTo(newGraph.getExecutions().get(0)));
    assertTrue(fusedGraph.getExecutions().get(1).isEquivalentTo(newGraph.getExecutions().get(1)));
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void simpleParallelGraphFullyReusable() {
    SimpleTestStepWithOutput oldStep1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep oldStep2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph oldGraph = parallel(OLD_JOB_ID,
        oldStep1, oldStep2);

    SimpleTestStepWithOutput newStep1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep newStep2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph newGraph = parallel(NEW_JOB_ID,
        //NOTE: Explicitly changed the order of the two parallel branches
        newStep2, newStep1);

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(2, fusedGraph.getExecutions().size());
    assertTrue(fusedGraph.getExecutions().stream().allMatch(step -> step instanceof DelegateStep));
    assertTrue(oldGraph.isEquivalentTo(newGraph));
    assertTrue(fusedGraph.isEquivalentTo(oldGraph));
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void simpleParallelGraphPartiallyReusable() {
    SimpleTestStepWithOutput oldStep1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep oldStep2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph oldGraph = parallel(OLD_JOB_ID,
        oldStep1, oldStep2);

    SimpleTestStepWithOutput newStep1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep newStep2 = new SimpleTestStep("SomeOtherStep");
    StepGraph newGraph = parallel(NEW_JOB_ID,
        //NOTE: Explicitly changed the order of the two parallel branches
        newStep2, newStep1);

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(2, fusedGraph.getExecutions().size());
    assertEquals(1, fusedGraph.getExecutions().stream().filter(step -> step instanceof DelegateStep).count());
    assertEquals(1, fusedGraph.getExecutions().stream().filter(step -> step.isEquivalentTo(oldGraph.getExecutions().get(0))).count());
    assertEquals(1, fusedGraph.getExecutions().stream().filter(step -> step.isEquivalentTo(newGraph.getExecutions().get(0))).count());
    assertNotEquals(1, fusedGraph.getExecutions().stream().filter(step -> step.isEquivalentTo(oldGraph.getExecutions().get(1))).count());
    assertEquals(1, fusedGraph.getExecutions().stream().filter(step -> step.isEquivalentTo(newGraph.getExecutions().get(1))).count());
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void simpleParallelGraphNotReusable() {
    SimpleTestStepWithOutput oldStep1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep oldStep2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph oldGraph = parallel(OLD_JOB_ID,
        oldStep1, oldStep2);

    SimpleTestStepWithOutput newStep1 = new SimpleTestStepWithOutput("SomeOtherExport");
    SimpleTestStep newStep2 = new SimpleTestStep("SomeOtherStep");
    StepGraph newGraph = parallel(NEW_JOB_ID,
        newStep1, newStep2);

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(2, fusedGraph.getExecutions().size());
    assertTrue(fusedGraph.getExecutions().stream().noneMatch(step -> step instanceof DelegateStep));
    assertFalse(fusedGraph.getExecutions().stream().anyMatch(step -> step.isEquivalentTo(oldGraph.getExecutions().get(0))));
    assertFalse(fusedGraph.getExecutions().stream().anyMatch(step -> step.isEquivalentTo(oldGraph.getExecutions().get(1))));
    assertEquals(1, fusedGraph.getExecutions().stream().filter(step -> step.isEquivalentTo(newGraph.getExecutions().get(0))).count());
    assertEquals(1, fusedGraph.getExecutions().stream().filter(step -> step.isEquivalentTo(newGraph.getExecutions().get(1))).count());
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void simpleCombinedGraphFullyReusable() {
    SimpleTestStepWithOutput oldProducerBranch1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep oldConsumerBranch1 = new SimpleTestStep(SOME_CONSUMER);
    SimpleTestStepWithOutput oldProducerBranch2 = new SimpleTestStepWithOutput(SOME_OTHER_EXPORT);
    SimpleTestStep oldConsumerBranch2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph oldGraph = parallel(OLD_JOB_ID,
        sequential(oldProducerBranch1, step(oldConsumerBranch1, inputsOf(oldProducerBranch1))),
        sequential(oldProducerBranch2, step(oldConsumerBranch2, inputsOf(oldProducerBranch2)))
    );

    SimpleTestStepWithOutput newProducerBranch1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep newConsumerBranch1 = new SimpleTestStep(SOME_CONSUMER);
    SimpleTestStepWithOutput newProducerBranch2 = new SimpleTestStepWithOutput(SOME_OTHER_EXPORT);
    SimpleTestStep newConsumerBranch2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph newGraph = parallel(NEW_JOB_ID,
        sequential(newProducerBranch1, step(newConsumerBranch1, inputsOf(newProducerBranch1))),
        sequential(newProducerBranch2, step(newConsumerBranch2, inputsOf(newProducerBranch2)))
    );

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(4, fusedGraph.size());
    assertTrue(fusedGraph.stepStream().allMatch(step -> step instanceof DelegateStep));
    assertTrue(oldGraph.isEquivalentTo(newGraph));
    assertTrue(fusedGraph.isEquivalentTo(oldGraph));
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void simpleCombinedGraphPartiallyReusable() {
    SimpleTestStepWithOutput oldProducerBranch1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep oldConsumerBranch1 = new SimpleTestStep(SOME_CONSUMER);
    SimpleTestStepWithOutput oldProducerBranch2 = new SimpleTestStepWithOutput(SOME_OTHER_EXPORT);
    SimpleTestStep oldConsumerBranch2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph oldGraph = parallel(OLD_JOB_ID,
        sequential(oldProducerBranch1, step(oldConsumerBranch1, inputsOf(oldProducerBranch1))),
        sequential(oldProducerBranch2, step(oldConsumerBranch2, inputsOf(oldProducerBranch2)))
    );

    SimpleTestStepWithOutput newProducerBranch1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep newConsumerBranch1 = new SimpleTestStep(SOME_CONSUMER);
    SimpleTestStepWithOutput newProducerBranch2 = new SimpleTestStepWithOutput("AgainSomeOtherExport");
    SimpleTestStep newConsumerBranch2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph newGraph = parallel(NEW_JOB_ID,
        sequential(newProducerBranch1, step(newConsumerBranch1, inputsOf(newProducerBranch1))),
        sequential(newProducerBranch2, step(newConsumerBranch2, inputsOf(newProducerBranch2)))
    );

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(4, fusedGraph.size());
    assertEquals(2, fusedGraph.stepStream().filter(step -> step instanceof DelegateStep).count());

    assertEquals(1, fusedGraph.getExecutions().stream().filter(branch -> branch.isEquivalentTo(oldGraph.getExecutions().get(0))).count());
    assertEquals(1, fusedGraph.getExecutions().stream().filter(branch -> branch.isEquivalentTo(newGraph.getExecutions().get(0))).count());
    assertNotEquals(1, fusedGraph.getExecutions().stream().filter(branch -> branch.isEquivalentTo(oldGraph.getExecutions().get(1))).count());
    assertEquals(1, fusedGraph.getExecutions().stream().filter(branch -> branch.isEquivalentTo(newGraph.getExecutions().get(1))).count());
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void simpleCombinedGraphNotReusable() {
    SimpleTestStepWithOutput oldProducerBranch1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep oldConsumerBranch1 = new SimpleTestStep(SOME_CONSUMER);
    SimpleTestStepWithOutput oldProducerBranch2 = new SimpleTestStepWithOutput(SOME_OTHER_EXPORT);
    SimpleTestStep oldConsumerBranch2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph oldGraph = parallel(OLD_JOB_ID,
        sequential(oldProducerBranch1, step(oldConsumerBranch1, inputsOf(oldProducerBranch1))),
        sequential(oldProducerBranch2, step(oldConsumerBranch2, inputsOf(oldProducerBranch2)))
    );

    SimpleTestStepWithOutput newProducerBranch1 = new SimpleTestStepWithOutput("AgainSomeOtherExportA");
    SimpleTestStep newConsumerBranch1 = new SimpleTestStep(SOME_CONSUMER);
    SimpleTestStepWithOutput newProducerBranch2 = new SimpleTestStepWithOutput("AgainSomeOtherExportB");
    SimpleTestStep newConsumerBranch2 = new SimpleTestStep(SOME_CONSUMER);
    StepGraph newGraph = parallel(NEW_JOB_ID,
        sequential(newProducerBranch1, step(newConsumerBranch1, inputsOf(newProducerBranch1))),
        sequential(newProducerBranch2, step(newConsumerBranch2, inputsOf(newProducerBranch2)))
    );

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(4, fusedGraph.size());
    assertTrue(fusedGraph.stepStream().noneMatch(step -> step instanceof DelegateStep));
    assertFalse(fusedGraph.getExecutions().stream().anyMatch(branch -> branch.isEquivalentTo(oldGraph.getExecutions().get(0))));
    assertFalse(fusedGraph.getExecutions().stream().anyMatch(branch -> branch.isEquivalentTo(oldGraph.getExecutions().get(1))));
    assertEquals(1, fusedGraph.getExecutions().stream().filter(branch -> branch.isEquivalentTo(newGraph.getExecutions().get(0))).count());
    assertEquals(1, fusedGraph.getExecutions().stream().filter(branch -> branch.isEquivalentTo(newGraph.getExecutions().get(1))).count());
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @CartesianTest
  public void newComplexGraphPartiallyReusable(@Values(booleans = {true, false}) boolean oldIsComplex) {
    SimpleTestStepWithOutput simpleProducer = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStep simpleConsumer = new SimpleTestStep(SOME_CONSUMER);
    StepGraph simpleGraph = sequential(
        simpleProducer,
        step(simpleConsumer, inputsOf(simpleProducer))
    );

    SimpleTestStepWithOutput complexProducer1 = new SimpleTestStepWithOutput(SOME_EXPORT);
    SimpleTestStepWithOutput complexProducer2 = new SimpleTestStepWithOutput(SOME_OTHER_EXPORT);
    SimpleTestStep complexConsumer = new SimpleTestStep(SOME_CONSUMER);
    StepGraph complexGraph = sequential(
        parallel(complexProducer1, complexProducer2),
        step(complexConsumer, inputsOf(complexProducer1, complexProducer2))
    );

    StepGraph oldGraph = ((CompilationStepGraph) (oldIsComplex ? complexGraph : simpleGraph)).enrich(OLD_JOB_ID);
    StepGraph newGraph = ((CompilationStepGraph) (oldIsComplex ? simpleGraph : complexGraph)).enrich(NEW_JOB_ID);

    StepGraph fusedGraph = fuse(newGraph, oldGraph);

    assertEquals(newGraph.size(), fusedGraph.size());
    assertEquals(1, fusedGraph.stepStream().filter(step -> step instanceof DelegateStep).count());
    assertTrue(fusedGraph.getStep(oldIsComplex ? simpleProducer.getId() : complexProducer1.getId()) instanceof DelegateStep);
    checkInputs(fusedGraph, newGraph);
    checkOutputs(fusedGraph, OLD_JOB_ID);
  }

  @Test
  public void canonicalizeParallelyWrappedSequentialGraphs() {
    StepGraph graph = parallel(
        sequential(
            new SimpleTestStep<>(SOME_EXPORT).withNotReusable(true),
            new SimpleTestStepWithOutput(SOME_EXPORT)
        ),
        sequential(
            new SimpleTestStep<>(SOME_OTHER_EXPORT).withNotReusable(true),
            new SimpleTestStepWithOutput(SOME_OTHER_EXPORT)
        )
    );

    StepGraph canonicalGraph = canonicalize(graph);

    assertTrue(canonicalGraph.isParallel());
    assertEquals(2, canonicalGraph.getExecutions().size());
  }

  @Test
  public void canonicalizeWrappedParallelGraph() {
    StepGraph graph = sequential(
        parallel(
            new SimpleTestStepWithOutput(SOME_EXPORT),
            new SimpleTestStepWithOutput(SOME_OTHER_EXPORT)
        )
    );

    StepGraph canonicalGraph = canonicalize(graph);

    assertTrue(canonicalGraph.isParallel());
    assertEquals(2, canonicalGraph.getExecutions().size());
  }

  @Test
  public void canonicalizeGraph() {
    StepGraph graph = parallel(
        sequential(
            sequential(
                sequential(
                    new SimpleTestStep(SOME_EXPORT).withNotReusable(true),
                    new SimpleTestStep(SOME_CONSUMER),
                    new SimpleTestStep(SOME_CONSUMER)
                )
            )
        ),
        sequential(
            new SimpleTestStep(SOME_CONSUMER),
            new SimpleTestStep(SOME_EXPORT).withNotReusable(true),
            new SimpleTestStep(SOME_CONSUMER)
        ),
        sequential(
            new SimpleTestStep(SOME_EXPORT).withNotReusable(true)
        )
    );

    StepGraph canonicalGraph = canonicalize(graph);
    assertEquals(2, canonicalGraph.getExecutions().size());
    assertEquals(2, ((StepGraph) canonicalGraph.getExecutions().get(0)).getExecutions().size());
    assertEquals(2, ((StepGraph) canonicalGraph.getExecutions().get(1)).getExecutions().size());
    assertTrue(((StepGraph) canonicalGraph.getExecutions().get(0)).stepStream().noneMatch(step -> ((SimpleTestStep) step).paramA == SOME_EXPORT));
    assertTrue(((StepGraph) canonicalGraph.getExecutions().get(1)).stepStream().noneMatch(step -> ((SimpleTestStep) step).paramA == SOME_EXPORT));
  }

  /*
  TODO: Add edge case tests:

  - compare different length of sequential graphs (both direction)
  - compare different length of sequential branches (both direction)
  - compare graphs which have different parallelity
   */

  //------------------------------ Helper Methods -------------------------------------------------


  private static StepGraph fuse(StepGraph newGraph, StepGraph oldGraph) {
    StepGraph fusedGraph = fuseGraphs(NEW_JOB_ID, newGraph, oldGraph);
    printGraphs(oldGraph, newGraph, fusedGraph);
    return fusedGraph;
  }

  private static void checkInputs(StepGraph fusedGraph, StepGraph newGraph) {
    fusedGraph.stepStream()
        .forEach(fusedStep ->  {
          //The number of input-sets of all steps in the fused graph must be equal to the number of input-sets of the according newly compiled step
          assertEquals(newGraph.getStep(fusedStep.getId()).getInputSets().size(), fusedStep.getInputSets().size(), "The number of input-sets of step \"" + fusedStep.getId() + "\" must match the number of input-sets in the according newly compiled step.");

          //For every input-set of the newly compiled step, there must exist an input-set with the same name in the according step of the fused graph
          ((List<InputSet>) newGraph.getStep(fusedStep.getId()).getInputSets()).forEach(compiledInputSet -> {
            assertTrue(fusedStep.getInputSets().stream().anyMatch(fusedInputSet -> ((InputSet) fusedInputSet).name().equals(compiledInputSet.name())), "There must exist a delegating input-set with name \"" + compiledInputSet.name() + "\" in step \"" + fusedStep.getId() + "\".");
          });

          //CHECK THAT ALL INPUTS OF THE FUSED GRAPH THAT *SHOULD* BE DELEGATED ACTUALLY *ARE* DELEGATED
          //For every input-set of all steps in the fused graph it must not be the case that it references a DelegateStep, because that would mean that the input-set was not delegated correctly to the old output
          ((List<InputSet>) fusedStep.getInputSets()).forEach(inputSet -> {
            if (inputSet.providerId() != null) {
              Step referencedStep = fusedGraph.getStep(inputSet.providerId());
              if (referencedStep != null) //NOTE: In case referencedStep == null that would mean that the step is not part of the fusedGraph, thus an old step would be referenced
                assertFalse(referencedStep instanceof DelegateStep, !(referencedStep instanceof DelegateStep) ? null : "The input-set \"" + inputSet.name() + "\" of step \"" + fusedStep.getId()
                    + "\" must be delegated to the old output-set of step \"" + ((DelegateStep) referencedStep).getDelegate().getGlobalStepId()
                    + "\" properly.");
            }
          });
        });
  }

  private static void checkOutputs(StepGraph fusedGraph, String oldJobId) {
    fusedGraph.stepStream()
        .forEach(step -> {
          if (step instanceof DelegateStep delegateStep) {
            //The number of output-sets of the new delegating step should be equal to the number of output-sets of the re-used (old) step
            assertEquals(delegateStep.getDelegate().getOutputSets().size(), delegateStep.getOutputSets().size());

            //For every output-set of the newly compiled step, there must exist an according output-set in the delegating step. That output-set must have the same name but the old job id.
            delegateStep.getDelegator().getOutputSets().forEach(compiledOutputSet -> {
              OutputSet accordingDelegatingOutputSet = delegateStep.getOutputSets().stream().filter(delegatingOutputSet -> delegatingOutputSet.name.equals(compiledOutputSet.name)).findFirst().orElse(null);
              assertTrue(accordingDelegatingOutputSet != null, "There must exist a delegating output-set with the name \"" + compiledOutputSet.name + "\" in the delegating step \"" + delegateStep.getId() + "\"");
              assertEquals(oldJobId, accordingDelegatingOutputSet.getJobId(), "The delegating output-set with name \"" + accordingDelegatingOutputSet + "\" in the delegating step \"" + delegateStep.getId() + "\" must have the old job ID");
            });

            //All outputs of a delegating step must have the same visibility as their counterpart in the newly compiled step
            delegateStep.getOutputSets().forEach(delegatingOutputSet -> {
              OutputSet accordingCompiledOutputSet = delegateStep.getDelegator().getOutputSets().stream().filter(compiledOutputSet -> compiledOutputSet.name.equals(delegatingOutputSet.name)).findFirst().get();
              assertEquals(accordingCompiledOutputSet.visibility, delegatingOutputSet.visibility, "Visibility of re-used output-set \"" + delegatingOutputSet.name + "\" in delegating step \"" + delegateStep.getId() + "\" must match the visibility of the newly compiled step.");
            });
          }
        });
  }

  private static void printGraphs(StepGraph oldGraph, StepGraph newGraph, StepGraph fusedGraph) {
    if (!printGraphs)
      return;
    System.out.println("Old: " + XyzSerializable.serialize(oldGraph, Internal.class));
    System.out.println("New: " + XyzSerializable.serialize(newGraph, Internal.class));
    System.out.println("Fused: " + XyzSerializable.serialize(fusedGraph, Internal.class));
  }

  private static StepGraph parallel(String jobId, StepExecution... steps) {
    return createGraph(true, steps).enrich(jobId);
  }

  private static StepGraph parallel(StepExecution... steps) {
    return createGraph(true, steps);
  }

  private static StepGraph sequential(String jobId, StepExecution... steps) {
    return createGraph(false, steps).enrich(jobId);
  }

  private static StepGraph sequential(StepExecution... steps) {
    return createGraph(false, steps);
  }

  private static CompilationStepGraph createGraph(boolean parallel, StepExecution... steps) {
    return (CompilationStepGraph) new CompilationStepGraph()
        .withParallel(parallel)
        .withExecutions(Arrays.stream(steps).collect(Collectors.toList()));
  }

  /**
   * Provides all outputs of the specified steps in InputsOf as inputs to the provided step.
   * That is what is normally done by the Job compiler in the real scenario.
   * @param step
   * @param inputs
   * @return
   */
  private static Step step(Step step, InputsOf inputs) {
    return step.withInputSets(Arrays.stream(inputs.steps)
        .flatMap(producerStep -> producerStep.getOutputSets().stream()
            .map(outputSet -> new InputSet(producerStep.getJobId(), producerStep.getId(), ((OutputSet) outputSet).name, ((OutputSet) outputSet).modelBased, producerStep.getOutputMetadata()))).toList());
  }

  private record InputsOf(Step... steps) {}

  private static InputsOf inputsOf(Step... steps) {
    return new InputsOf(steps);
  }
}
