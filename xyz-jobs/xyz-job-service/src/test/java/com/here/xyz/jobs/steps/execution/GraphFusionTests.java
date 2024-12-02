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

import static com.here.xyz.jobs.steps.execution.GraphFusionTool.fuseGraphs;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStep;
import com.here.xyz.jobs.steps.execution.fusion.SimpleTestStepWithOutput;
import java.util.Arrays;
import java.util.stream.Collectors;
import org.junit.Test;

public class GraphFusionTests {

  public static final String SOME_EXPORT = "SomeExport";
  public static final String SOME_CONSUMER = "SomeConsumer";
  public static final String NEW_JOB_ID = "newJob";
  public static final String OLD_JOB_ID = "oldJob";

  private static StepGraph parallel(String jobId, Step... steps) {
    return sequential(jobId, steps).withParallel(true);
  }

  private static StepGraph sequential(String jobId, Step... steps) {
    return ((CompilationStepGraph) new CompilationStepGraph()
        .withExecutions(Arrays.stream(steps).collect(Collectors.toList()))).enrich(jobId);
  }

  @Test
  public void simpleSequentialGraphFullyReusable() {
    StepGraph oldGraph = sequential(OLD_JOB_ID,
        new SimpleTestStepWithOutput(SOME_EXPORT), new SimpleTestStep(SOME_CONSUMER));

    StepGraph newGraph = sequential(NEW_JOB_ID,
        new SimpleTestStepWithOutput(SOME_EXPORT), new SimpleTestStep(SOME_CONSUMER));

    StepGraph fusedGraph = fuseGraphs(NEW_JOB_ID, oldGraph, newGraph);

    printGraphs(oldGraph, newGraph, fusedGraph);

    assertTrue(oldGraph.isEquivalentTo(newGraph));
    assertTrue(fusedGraph.isEquivalentTo(oldGraph));
    assertTrue(fusedGraph.getExecutions().stream().allMatch(step -> step instanceof DelegateStep));
  }

  @Test
  public void simpleSequentialGraphPartiallyReusable() {
    StepGraph oldGraph = sequential(OLD_JOB_ID,
        new SimpleTestStepWithOutput(SOME_EXPORT), new SimpleTestStep(SOME_CONSUMER));

    StepGraph newGraph = sequential(NEW_JOB_ID,
        new SimpleTestStepWithOutput(SOME_EXPORT), new SimpleTestStep("SomeOtherStep"));

    StepGraph fusedGraph = fuseGraphs(NEW_JOB_ID, oldGraph, newGraph);

    printGraphs(oldGraph, newGraph, fusedGraph);

    assertTrue(fusedGraph.getExecutions().get(0).isEquivalentTo(fusedGraph.getExecutions().get(0)));
    assertFalse(fusedGraph.getExecutions().get(1).isEquivalentTo(fusedGraph.getExecutions().get(1)));

  }

  @Test
  public void simpleSequentialGraphNotReusable() {

  }

  private static void printGraphs(StepGraph oldGraph, StepGraph newGraph, StepGraph fusedGraph) {
    System.out.println("Old: " + XyzSerializable.serialize(oldGraph));
    System.out.println("New: " + XyzSerializable.serialize(newGraph));
    System.out.println("Fused: " + XyzSerializable.serialize(fusedGraph));
  }

}
