package com.here.xyz.jobs.com.here.xyz.jobs.steps.execution;

import com.here.xyz.jobs.Job;
import com.here.xyz.jobs.service.Config;
import com.here.xyz.jobs.steps.CompilationStepGraph;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.jobs.steps.StepExecution;
import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.DelegateOutputsPseudoStep;
import com.here.xyz.jobs.steps.impl.transport.ExportSpaceToFiles;
import com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.here.xyz.jobs.steps.impl.transport.ImportFilesToSpace.Format.GEOJSON;

public class JobExecutor {
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

        CompilationStepGraph graph1 = connectStepExecutions(List.of(
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
        ),false);
        graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

        CompilationStepGraph graph2 = connectStepExecutions(List.of(
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2) //not reusable
        ),false);
        graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID1);

        StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
        //2 Steps can be reused
        Assertions.assertEquals(2, connectedEquivalentSubGraph.getExecutions().size());

        com.here.xyz.jobs.steps.execution.JobExecutor.shrinkGraphByReusingOtherGraph(
                new Job().withId(JOB_ID1).withSteps(graph2),
                connectedEquivalentSubGraph
        ).onSuccess(graph ->
                {
                    Step execution1 = (Step)graph.getExecutions().get(0);
                    Step execution2 = (Step)graph.getExecutions().get(1);
                    Step execution3 = (Step)graph.getExecutions().get(2);

                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution1);
                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution2);
                    Assertions.assertInstanceOf(ExportSpaceToFiles.class, execution3);

                    //Check if previousStepId is set correct
                    Assertions.assertEquals(new HashSet<>(List.of(execution2.getId()))
                            , execution3.getPreviousStepIds());
                }
        );
    }

    @Test
    public void testReuseSequentialGraphWithInputIds(){
        //      |   (reusable)
        //      |   (reusable)
        //      |   (not-reusable)
        ExportSpaceToFiles exportStep = (ExportSpaceToFiles)stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1);

        CompilationStepGraph graph1 = connectStepExecutions(List.of(
                exportStep,
                stepGenerator(ImportFilesToSpace.class, JOB_ID1, SOURCE_ID2, Set.of(exportStep.getId()))
        ),false);
        graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

        ExportSpaceToFiles exportStep2 = (ExportSpaceToFiles)stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1);
        CompilationStepGraph graph2 = connectStepExecutions(List.of(
                exportStep2,
                stepGenerator(ImportFilesToSpace.class, JOB_ID2, SOURCE_ID2, Set.of(exportStep2.getId())) //not reusable
        ),false);
        graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID1);

        StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
        //Only ExportStep can be reused. Imports are not implementing isEquivalentTo().
        Assertions.assertEquals(1, connectedEquivalentSubGraph.getExecutions().size());

        com.here.xyz.jobs.steps.execution.JobExecutor.shrinkGraphByReusingOtherGraph(
                new Job().withId(JOB_ID1).withSteps(graph1),
                connectedEquivalentSubGraph
        ).onSuccess(graph ->
                {
                    Step execution1 = (Step)graph.getExecutions().get(0);
                    Step execution2 = (Step)graph.getExecutions().get(1);

                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution1);
                    Assertions.assertInstanceOf(ImportFilesToSpace.class, execution2);

                    //Check if previousStepIds are set correct
                    Assertions.assertEquals(new HashSet<>(List.of(execution1.getId()))
                            , execution2.getPreviousStepIds());
                    //Check if inputStepIds are set correct (+ linked to reused job)
                    Assertions.assertEquals(new HashSet<>(List.of(JOB_ID1 +"."+ exportStep.getId()))
                            , execution2.getInputStepIds());
                }
        );
    }

    @Test
    public void testReuseParallelGraph(){
        //      |(reusable)  |(reusable)  |(not-reusable)

        CompilationStepGraph graph1 = connectStepExecutions(List.of(
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
        ),true);
        graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

        CompilationStepGraph graph2 = connectStepExecutions(List.of(
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2) //not reusable
        ),true);
        graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID2);

        StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
        //2 Steps can be reused
        Assertions.assertEquals(2, connectedEquivalentSubGraph.getExecutions().size());

        com.here.xyz.jobs.steps.execution.JobExecutor.shrinkGraphByReusingOtherGraph(
                new Job().withId(JOB_ID1).withSteps(graph2),
                connectedEquivalentSubGraph
        ).onSuccess(graph ->
                {
                    Assertions.assertTrue(graph.isParallel());
                    Step execution1 = (Step)graph.getExecutions().get(0);
                    Step execution2 = (Step)graph.getExecutions().get(1);
                    Step execution3 = (Step)graph.getExecutions().get(2);

                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution1);
                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution2);
                    Assertions.assertInstanceOf(ExportSpaceToFiles.class, execution3);

                    //Check if previousStepId is set correct
                    graph.getExecutions().forEach(step ->{
                        Assertions.assertTrue(((Step)step).getPreviousStepIds().isEmpty());
                    });
                }
        );
    }

    @Test
    public void testReuseParallelAndSequentialGraph(){
        //      |  |  | (all reusable)
        //      _______
        //         |    (reusable)
        //         |    (not-reusable)

        CompilationStepGraph graph1 = connectStepExecutions(List.of(
                connectStepExecutions(List.of(
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
                ),true),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
        ),false);
        graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

        CompilationStepGraph graph2 = connectStepExecutions(List.of(
                connectStepExecutions(List.of(
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
        Assertions.assertEquals(4, com.here.xyz.jobs.steps.execution.JobExecutor.getAllLeafExecutionNodes(connectedEquivalentSubGraph).size());

        com.here.xyz.jobs.steps.execution.JobExecutor.shrinkGraphByReusingOtherGraph(
                new Job().withId(JOB_ID1).withSteps(graph2),
                connectedEquivalentSubGraph
        ).onSuccess(graph ->
                {

                    StepGraph execution1Graph = (StepGraph)graph.getExecutions().get(0);
                    Assertions.assertTrue(execution1Graph.isParallel());
                    execution1Graph.getExecutions().forEach(step -> Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, step));

                    Step execution2 = (Step)graph.getExecutions().get(1);
                    Step execution3 = (Step)graph.getExecutions().get(2);

                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution2);
                    Assertions.assertInstanceOf(ExportSpaceToFiles.class, execution3);

                    //Check if previousStepId is set correct
                    Assertions.assertEquals(new HashSet<>(List.of(execution2.getId()))
                            , execution3.getPreviousStepIds());
                }
        );
    }

    //TODO: Verify if behavior is correct
    @Test
    public void testReuseParallelAndSequentialGraph2(){
        //      |  |  | (reusable) | (not-reusable)
        //      _______
        //         |    (reusable)

        CompilationStepGraph graph1 = connectStepExecutions(List.of(
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
        ),false);
        graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

        CompilationStepGraph graph2 = connectStepExecutions(List.of(
                connectStepExecutions(List.of(
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2)
                ),true),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1)
        ),false);
        graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID2);

        StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
        //4 Steps can be potentially reused
        Assertions.assertEquals(0, com.here.xyz.jobs.steps.execution.JobExecutor.getAllLeafExecutionNodes(connectedEquivalentSubGraph).size());

        com.here.xyz.jobs.steps.execution.JobExecutor.shrinkGraphByReusingOtherGraph(
                new Job().withId(JOB_ID1).withSteps(graph2),
                connectedEquivalentSubGraph
        ) .onSuccess(graph -> Assertions.assertNull(graph));
    }

    @Test
    public void testReuseMixedGraph(){
        //    |      (reusable)
        //   - -
        //   | |     (all reusable)
        //   - -
        //    |      (not-reusable)
        //    |      (not-reusable)

        CompilationStepGraph graph1 = connectStepExecutions(List.of(
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                connectStepExecutions(List.of(
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
                ),true),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
        ),false);
        graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

        CompilationStepGraph graph2 = connectStepExecutions(List.of(
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
                connectStepExecutions(List.of(
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1)
                ),true),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2), //not reusable
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2)  //not reusable
        ),false);
        graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID2);

        StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
        //3 Steps can be potentially reused
        Assertions.assertEquals(3, com.here.xyz.jobs.steps.execution.JobExecutor.getAllLeafExecutionNodes(connectedEquivalentSubGraph).size());

        com.here.xyz.jobs.steps.execution.JobExecutor.shrinkGraphByReusingOtherGraph(
                new Job().withId(JOB_ID1).withSteps(graph2),
                connectedEquivalentSubGraph
        ).onSuccess(graph ->
                {

                    Step execution1 = (Step)graph.getExecutions().get(0);
                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution1);

                    StepGraph execution2Graph = (StepGraph)graph.getExecutions().get(1);
                    Assertions.assertTrue(execution2Graph.isParallel());
                    execution2Graph.getExecutions().forEach(step -> Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, step));
                    Set<String> execution2Ids = execution2Graph.getExecutions().stream().map(stepExecution -> ((Step) stepExecution).getId()).collect(Collectors.toSet());

                    Step execution3 = (Step)graph.getExecutions().get(2);
                    Step execution4 = (Step)graph.getExecutions().get(3);

                    Assertions.assertInstanceOf(ExportSpaceToFiles.class, execution3);
                    Assertions.assertInstanceOf(ExportSpaceToFiles.class, execution4);

                    //Check if previousStepId is set correct
                    Assertions.assertEquals(execution2Ids, execution3.getPreviousStepIds());
                }
        );
    }

    @Test
    public void testReuseMixedGraph2() {
        //    |      (reusable)
        //   - -
        //   | |     (left reusable & right not)
        //   - -
        //    |      (reusable)

        CompilationStepGraph graph1 = connectStepExecutions(List.of(
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                connectStepExecutions(List.of(
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1),
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
                ),true),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID1, SOURCE_ID1)
        ),false);
        graph1 = (CompilationStepGraph) graph1.enrich(JOB_ID1);

        CompilationStepGraph graph2 = connectStepExecutions(List.of(
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
                connectStepExecutions(List.of(
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1),
                        stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID2) //not reusable
                ),true),
                stepGenerator(ExportSpaceToFiles.class, JOB_ID2, SOURCE_ID1)
        ),false);
        graph2 = (CompilationStepGraph) graph2.enrich(JOB_ID2);


        StepGraph connectedEquivalentSubGraph = graph2.findConnectedEquivalentSubGraph(graph1);
        //3 Steps can be potentially reused
        Assertions.assertEquals(3, com.here.xyz.jobs.steps.execution.JobExecutor.getAllLeafExecutionNodes(connectedEquivalentSubGraph).size());

        com.here.xyz.jobs.steps.execution.JobExecutor.shrinkGraphByReusingOtherGraph(
                new Job().withId(JOB_ID1).withSteps(graph2),
                connectedEquivalentSubGraph
        ).onSuccess(graph ->
                {

                    Step execution1 = (Step)graph.getExecutions().get(0);
                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution1);

                    StepGraph execution2Graph = (StepGraph)graph.getExecutions().get(1);
                    Assertions.assertTrue(execution2Graph.isParallel());
                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution2Graph.getExecutions().get(0));
                    Assertions.assertInstanceOf(ExportSpaceToFiles.class, execution2Graph.getExecutions().get(1));

                    Step execution3 = (Step)graph.getExecutions().get(2);
                    Assertions.assertInstanceOf(DelegateOutputsPseudoStep.class, execution3);

                    //Check if previousStepId is set correct
                    //TODO: verify if empty previousStepIds are ok for DelegateOutputsPseudoStep
                    Assertions.assertTrue(((Step)execution2Graph.getExecutions().get(0)).getPreviousStepIds().isEmpty());
                    Assertions.assertEquals(Set.of(execution1.getId()), ((Step)execution2Graph.getExecutions().get(1)).getPreviousStepIds());
                }
        );
    }

    public static CompilationStepGraph connectStepExecutions(List<StepExecution> stepExecutions, boolean isParallel) {
        CompilationStepGraph compilationStepGraph = new CompilationStepGraph();

        stepExecutions.forEach(stepExecution -> {
            compilationStepGraph.addExecution(stepExecution);
        });

        compilationStepGraph.setParallel(isParallel);

        return compilationStepGraph;
    }

    public StepExecution stepGenerator(Class stepType, String jobId, String sourceId) {
        return stepGenerator(stepType, jobId, sourceId, null);
    }

    public StepExecution stepGenerator(Class stepType, String jobId, String sourceId, Set<String> inputStepIds){

        return switch (stepType.getSimpleName()) {
            case "ExportSpaceToFiles" -> new ExportSpaceToFiles()
                    .withSpaceId(sourceId)
                    .withJobId(jobId)
                    .withUseSystemOutput(true);

            case "ImportFilesToSpace" -> {
                ImportFilesToSpace importFilesToSpace = new ImportFilesToSpace()
                        .withFormat(GEOJSON)
                        .withSpaceId(sourceId)
                        .withJobId(jobId);
                if(inputStepIds != null) {
                    importFilesToSpace.setInputStepIds(inputStepIds);
                    importFilesToSpace.setUseSystemInput(true);
                }
                yield importFilesToSpace;
            }
            default -> throw new IllegalStateException("Unexpected value: " + stepType);
        };
    }
}
