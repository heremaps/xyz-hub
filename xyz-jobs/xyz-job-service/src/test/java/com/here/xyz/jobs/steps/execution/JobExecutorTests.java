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
import static com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles.EXPORTED_DATA;
import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.GEOJSON;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.Step.InputSet;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class JobExecutorTests {
  private static final String JOB_ID1 = "TEST_JOB_1";
  private static final String JOB_ID2 = "TEST_JOB_2";
  private static final String SOURCE_ID1 = "SOURCE_1";
  private static final String SOURCE_ID2 = "SOURCE_2";

  {
    try {
      new Config();
      Config.instance.PARALLEL_STEPS_SUPPORTED = true;
      Config.instance.JOBS_DYNAMODB_TABLE_ARN = "arn:aws:dynamodb:localhost:000000008000:table/xyz-jobs-local";
      Config.instance.LOCALSTACK_ENDPOINT = new URI("http://localhost:4566");
      Config.instance.JOBS_S3_BUCKET = "test-bucket";
      Config.instance.AWS_REGION = "us-east-1";
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testReuseSequentialGraph(){
    //      |   (reusable)
    //      |   (reusable)
    //      |   (not-reusable)

    CompilationStepGraph graph1 = createGraph(List.of(
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
    ),false);
    graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

    CompilationStepGraph graph2 = createGraph(List.of(
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2) //not reusable
    ),false);
    graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID1);

    StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
    //2 Steps can be reused
    assertEquals(2, connectedEquivalentSubGraph.getExecutions().size());



    StepGraph graph = GraphFusionTool.fuseGraphs(
        new Job().withId(JOB_ID1).withSteps(graph2),
        connectedEquivalentSubGraph
    );
    Step execution1 = (Step)graph.getExecutions().get(0);
    Step execution2 = (Step)graph.getExecutions().get(1);
    Step execution3 = (Step)graph.getExecutions().get(2);

    assertInstanceOf(DelegateStep.class, execution1);
    assertInstanceOf(DelegateStep.class, execution2);
    assertInstanceOf(ExportSpaceToFiles.class, execution3);

    //Check if previousStepId is set correct
    assertEquals(new HashSet<>(List.of(execution2.getId()))
        , execution3.getPreviousStepIds());
  }

  @Test
  public void testReuseSequentialGraphWithInputIds() {
    //      |   (reusable)
    //      |   (reusable)
    //      |   (not-reusable)

    ExportSpaceToFiles exportStep1 = (ExportSpaceToFiles) stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1);
    StepGraph graph1 = new CompilationStepGraph()
        .withExecutions(List.of(
            exportStep1,
            stepGenerator(ImportFilesToSpace.class, JOB_ID1, SOURCE_ID2, Set.of(exportStep1.getId()))
        ))
        .withParallel(false);
    ((CompilationStepGraph) graph1).enrich(JOB_ID1);

    ExportSpaceToFiles exportStep2 = (ExportSpaceToFiles) stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1);
    //not reusable
    StepGraph graph2 = new CompilationStepGraph()
        .withExecutions(List.of(
            exportStep2,
            stepGenerator(ImportFilesToSpace.class, JOB_ID2, SOURCE_ID2, Set.of(exportStep2.getId())) //not reusable
        ))
        .withParallel(false);
    ((CompilationStepGraph) graph2).enrich(JOB_ID1);

    StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
    //Only ExportStep can be reused. Imports are not implementing isEquivalentTo().
    assertEquals(1, connectedEquivalentSubGraph.getExecutions().size());

    StepGraph graph = GraphFusionTool.fuseGraphs(
        new Job().withId(JOB_ID1).withSteps(graph1),
        connectedEquivalentSubGraph
    );

    Step execution1 = (Step) graph.getExecutions().get(0);
    Step execution2 = (Step) graph.getExecutions().get(1);

    assertInstanceOf(DelegateStep.class, execution1);
    assertInstanceOf(ImportFilesToSpace.class, execution2);

    //Check if previousStepIds are set correct
    assertEquals(Set.of(execution1.getId()), execution2.getPreviousStepIds());

    //Check if inputStepIds are set correct (+ linked to a re-used job)
    final InputSet reusedInputSet = ((List<InputSet>) execution2.getInputSets()).stream()
        .filter(inputSet -> Objects.equals(inputSet.name(), EXPORTED_DATA))
        .findAny()
        .get();
    assertEquals(reusedInputSet.jobId(), JOB_ID1);
    assertEquals(reusedInputSet.providerId(), exportStep2.getId());
  }

  @Test
  public void testReuseParallelGraph(){
    //      |(reusable)  |(reusable)  |(not-reusable)

    CompilationStepGraph graph1 = createGraph(List.of(
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
    ),true);
    graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

    CompilationStepGraph graph2 = createGraph(List.of(
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2) //not reusable
    ),true);
    graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID2);

    StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
    //2 Steps can be reused
    assertEquals(2, connectedEquivalentSubGraph.getExecutions().size());

    StepGraph graph = GraphFusionTool.fuseGraphs(
        new Job().withId(JOB_ID1).withSteps(graph2),
        connectedEquivalentSubGraph
    );

    Assertions.assertTrue(graph.isParallel());
    Step execution1 = (Step)graph.getExecutions().get(0);
    Step execution2 = (Step)graph.getExecutions().get(1);
    Step execution3 = (Step)graph.getExecutions().get(2);

    assertInstanceOf(DelegateStep.class, execution1);
    assertInstanceOf(DelegateStep.class, execution2);
    assertInstanceOf(ExportSpaceToFiles.class, execution3);

    //Check if previousStepId is set correct
    graph.getExecutions().forEach(step -> Assertions.assertTrue(((Step)step).getPreviousStepIds().isEmpty()));
  }

  @Test
  public void testReuseParallelAndSequentialGraph(){
    //      |  |  | (all reusable)
    //      _______
    //         |    (reusable)
    //         |    (not-reusable)

    CompilationStepGraph graph1 = createGraph(List.of(
        createGraph(List.of(
            stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
            stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
            stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
        ),true),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
    ),false);
    graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

    CompilationStepGraph graph2 = createGraph(List.of(
        createGraph(List.of(
            stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
            stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
            stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1)
        ),true),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2)
    ),false);
    graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID2);

    StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
    //4 Steps can be potentially reused
    Assertions.assertEquals(4, getAllLeafExecutionNodes(connectedEquivalentSubGraph).size());

    StepGraph graph = GraphFusionTool.fuseGraphs(
        new Job().withId(JOB_ID1).withSteps(graph2),
        connectedEquivalentSubGraph
    );

    StepGraph execution1Graph = (StepGraph)graph.getExecutions().get(0);
    Assertions.assertTrue(execution1Graph.isParallel());
    execution1Graph.getExecutions().forEach(step -> assertInstanceOf(DelegateStep.class, step));

    Step execution2 = (Step)graph.getExecutions().get(1);
    Step execution3 = (Step)graph.getExecutions().get(2);

    assertInstanceOf(DelegateStep.class, execution2);
    assertInstanceOf(ExportSpaceToFiles.class, execution3);

    //Check if previousStepId is set correct
    assertEquals(Set.of(execution2.getId()), execution3.getPreviousStepIds());
  }

  //TODO: Verify if behavior is correct
  @Test
  public void testReuseParallelAndSequentialGraph2(){
    //      |  |  | (reusable) | (not-reusable)
    //      _______
    //         |    (reusable)

    CompilationStepGraph graph1 = createGraph(List.of(
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
    ),false);
    graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

    CompilationStepGraph graph2 = createGraph(List.of(
        createGraph(List.of(
            stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
            stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2)
        ),true),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1)
    ),false);
    graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID2);

    StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
    //4 Steps can be potentially reused
    Assertions.assertEquals(0, getAllLeafExecutionNodes(connectedEquivalentSubGraph).size());

    StepGraph graph = GraphFusionTool.fuseGraphs(
        new Job().withId(JOB_ID1).withSteps(graph2),
        connectedEquivalentSubGraph
    );

    assertTrue(graph.stepStream().noneMatch(step -> step instanceof DelegateStep));
  }

  @Test
  public void testReuseMixedGraph(){
    //    |      (reusable)
    //   - -
    //   | |     (all reusable)
    //   - -
    //    |      (not-reusable)
    //    |      (not-reusable)

    CompilationStepGraph graph1 = createGraph(List.of(
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
        createGraph(List.of(
            stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
            stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
        ),true),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
    ),false);
    graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

    CompilationStepGraph graph2 = createGraph(List.of(
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
        createGraph(List.of(
            stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
            stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1)
        ),true),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2), //not reusable
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2)  //not reusable
    ),false);
    graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID2);

    StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
    //3 Steps can be potentially reused
    Assertions.assertEquals(3, getAllLeafExecutionNodes(connectedEquivalentSubGraph).size());

    StepGraph graph = GraphFusionTool.fuseGraphs(
        new Job().withId(JOB_ID1).withSteps(graph2),
        connectedEquivalentSubGraph
    );

    Step execution1 = (Step)graph.getExecutions().get(0);
    assertInstanceOf(DelegateStep.class, execution1);

    StepGraph execution2Graph = (StepGraph)graph.getExecutions().get(1);
    Assertions.assertTrue(execution2Graph.isParallel());
    execution2Graph.getExecutions().forEach(step -> assertInstanceOf(DelegateStep.class, step));
    Set<String> execution2Ids = execution2Graph.getExecutions().stream().map(stepExecution -> ((Step) stepExecution).getId()).collect(Collectors.toSet());

    Step execution3 = (Step)graph.getExecutions().get(2);
    Step execution4 = (Step)graph.getExecutions().get(3);

    assertInstanceOf(ExportSpaceToFiles.class, execution3);
    assertInstanceOf(ExportSpaceToFiles.class, execution4);

    //Check if previousStepId is set correct
    assertEquals(execution2Ids, execution3.getPreviousStepIds());
  }

  @Disabled("Check this test, IMO it's a wrong assertion that the 3rd sequential execution of the main-graph should be re-usable if the 2nd execution was not fully re-usable / not fully equivalent.")
  public void testReuseMixedGraph2() {
    //    |      (reusable)
    //   - -
    //   | |     (left reusable & right not)
    //   - -
    //    |      (reusable)

    CompilationStepGraph graph1 = createGraph(List.of(
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
        createGraph(List.of(
            stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
            stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
        ),true),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
    ),false);
    graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

    CompilationStepGraph graph2 = createGraph(List.of(
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
        createGraph(List.of(
            stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
            stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2) //not reusable
        ),true),
        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1)
    ),false);
    graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID2);


    StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
    //3 Steps can be potentially reused
    Assertions.assertEquals(3, getAllLeafExecutionNodes(connectedEquivalentSubGraph).size());

    StepGraph graph = GraphFusionTool.fuseGraphs(
        new Job().withId(JOB_ID1).withSteps(graph2),
        connectedEquivalentSubGraph
    );

    Step execution1 = (Step)graph.getExecutions().get(0);
    assertInstanceOf(DelegateStep.class, execution1);

    StepGraph execution2Graph = (StepGraph)graph.getExecutions().get(1);
    Assertions.assertTrue(execution2Graph.isParallel());
    assertInstanceOf(DelegateStep.class, execution2Graph.getExecutions().get(0));
    assertInstanceOf(ExportSpaceToFiles.class, execution2Graph.getExecutions().get(1));

    Step execution3 = (Step)graph.getExecutions().get(2);
    //TODO: The following assertion is incorrect IMO
    assertInstanceOf(DelegateStep.class, execution3);

    //Check if previousStepId is set correct
    //TODO: verify if empty previousStepIds are ok for DelegateStep
    Assertions.assertTrue(((Step)execution2Graph.getExecutions().get(0)).getPreviousStepIds().isEmpty());
    assertEquals(Set.of(execution1.getId()), ((Step)execution2Graph.getExecutions().get(1)).getPreviousStepIds());
  }

  //TODO: Remove that method and simply replace all occurances by: new CompilationStepGraph().withExecutions(stepExecutions).withParallel(isParallel)
  public static CompilationStepGraph createGraph(List<StepExecution> stepExecutions, boolean isParallel) {
    return (CompilationStepGraph) new CompilationStepGraph().withExecutions(stepExecutions).withParallel(isParallel);
  }

  public StepExecution stepGenerator(Class stepType, String jobId, String sourceId) {
    return stepGenerator(stepType, jobId, sourceId, null);
  }

  public StepExecution stepGenerator(Class stepType, String jobId, String sourceId, Set<String> inputStepIds) {

    return switch (stepType.getSimpleName()) {
      case "ExportSpaceToFiles" -> new ExportSpaceToFiles()
          .withSpaceId(sourceId)
          .withJobId(jobId)
          .withOutputSetVisibility(EXPORTED_DATA, SYSTEM);

      case "ImportFilesToSpace" -> {
        ImportFilesToSpace importFilesToSpace = new ImportFilesToSpace()
            .withFormat(GEOJSON)
            .withSpaceId(sourceId)
            .withJobId(jobId);
        if (inputStepIds != null)
          importFilesToSpace.setInputSets(inputStepIds.stream().map(stepId -> new InputSet(stepId, EXPORTED_DATA, false)).toList());

        yield importFilesToSpace;
      }
      default -> throw new IllegalStateException("Unexpected value: " + stepType);
    };
  }

  /**
   * Recursively retrieves all leaf nodes from the given {@code StepExecution}.
   *
   * <p>This method traverses a hierarchy of {@code StepExecution} objects. If the execution node
   * is a {@code StepGraph}, it recursively processes all its child executions. If the execution
   * node is a {@code Step}, it is considered a leaf node and added to the result.</p>
   *
   * <p>The method ensures that all terminal steps (leaves) in a potentially nested graph
   * structure are collected into a flat list.</p>
   *
   * @param execution the root {@code StepExecution} from which to collect all leaf nodes.
   *                      This may be a {@code Step} or a {@code StepGraph}.
   * @return a list of {@code Step} objects representing all leaf nodes in the execution graph.
   */
  @Deprecated
  public static List<Step> getAllLeafExecutionNodes(StepExecution execution) {
    List<Step> leafNodes = new ArrayList<>();

    if (execution instanceof StepGraph graph) {
      //Traverse all child executions in the graph
      for (StepExecution child : graph.getExecutions())
        leafNodes.addAll(getAllLeafExecutionNodes(child));
    }
    else if (execution instanceof Step)
      //If it's a leaf node, add it to the list
      leafNodes.add((Step) execution);

    return leafNodes;
  }
}
