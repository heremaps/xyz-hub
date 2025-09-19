package com.here.xyz.jobs;

import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.fusion.OutputSetsTestStep;
import com.here.xyz.jobs.steps.outputs.GroupSummary;
import com.here.xyz.jobs.steps.outputs.GroupedPayloadsPreview;
import com.here.xyz.jobs.steps.outputs.SetSummary;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class JobOutputsPreviewTest {

  @org.junit.BeforeClass
  public static void initVertx() {
    com.here.xyz.util.service.Core.vertx = Vertx.vertx();
  }

  @org.junit.AfterClass
  public static void closeVertx() {
    if (com.here.xyz.util.service.Core.vertx != null) {
      com.here.xyz.util.service.Core.vertx.close();
      com.here.xyz.util.service.Core.vertx = null;
    }
  }

  private static <T> T await(Future<T> f) throws Exception {
    return f.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
  }

  private Job buildJobForTest() {
    String jobId = "job-outputs-preview-test";

    Map<String, Integer> step1Counts = Map.of(
        "setA", 2,
        "setB", 1
    );
    Map<String, Integer> step2Counts = Map.of(
        "setA", 3,
        "setB", 4
    );

    OutputSetsTestStep step1 = new OutputSetsTestStep(jobId, "group1", step1Counts);
    OutputSetsTestStep step2 = new OutputSetsTestStep(jobId, "group1", step2Counts);

    StepGraph graph = new StepGraph();
    graph.addExecution(step1);
    graph.addExecution(step2);

    Job job = new Job().withId(jobId).withSteps(graph);
    job.withStatus(new RuntimeStatus());
    return job;
  }

  @Test
  public void composeOutputGroupPreview_shouldSumPerSetAcrossStepsCorrectly() throws Exception {
    Job job = buildJobForTest();

    GroupSummary group = await(job.composeOutputGroupPreview("group1"));

    Assert.assertEquals(10L, group.getItemCount());
    Assert.assertNotNull(group.getItems());
    Assert.assertEquals(2, group.getItems().size());
    Assert.assertEquals(5L, group.getItems().get("setA").getItemCount());
    Assert.assertEquals(5L, group.getItems().get("setB").getItemCount());

    Assert.assertEquals(0L, group.getByteSize());
  }

  @Test
  public void composeOutputsPreview_shouldAggregateGroupsAndSetsCorrectly() throws Exception {
    Job job = buildJobForTest();

    GroupedPayloadsPreview preview = await(job.composeOutputsPreview());

    Assert.assertEquals(10L, preview.getItemCount());
    Assert.assertNotNull(preview.getItems());
    Assert.assertEquals(1, preview.getItems().size());

    GroupSummary group1 = preview.getItems().get("group1");
    Assert.assertNotNull(group1);
    Assert.assertEquals(10L, group1.getItemCount());

    Map<String, SetSummary> sets = group1.getItems();
    Assert.assertEquals(2, sets.size());
    Assert.assertEquals(5L, sets.get("setA").getItemCount());
    Assert.assertEquals(5L, sets.get("setB").getItemCount());

    Assert.assertEquals(0L, preview.getByteSize());
    Assert.assertEquals(0L, group1.getByteSize());
  }
}
