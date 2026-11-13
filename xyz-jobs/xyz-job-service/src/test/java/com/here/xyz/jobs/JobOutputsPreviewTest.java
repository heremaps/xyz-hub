package com.here.xyz.jobs;

import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.fusion.OutputSetsTestStep;
import com.here.xyz.jobs.steps.GroupPayloads;
import com.here.xyz.jobs.steps.JobPayloads;
import com.here.xyz.jobs.steps.SetPayloads;
import com.here.xyz.jobs.steps.Step;
import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class JobOutputsPreviewTest {

  @BeforeClass
  public static void initVertx() {
    //Do not close it afterwards: Static Async instances (e.g., of Job) keep using their worker executors in other tests of the fork
    if (Core.vertx == null)
      Core.vertx = Vertx.vertx();
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

    GroupPayloads group = await(job.composeOutputGroupPreview("group1"));

    Assert.assertEquals(10L, group.getItemCount());
    Assert.assertNotNull(group.getSets());
    Assert.assertEquals(2, group.getSets().size());
    Assert.assertEquals(5L, group.getSets().get("setA").getItemCount());
    Assert.assertEquals(5L, group.getSets().get("setB").getItemCount());

    Assert.assertEquals(0L, group.getByteSize());
  }

  @Test
  public void composeOutputsPreview_shouldAggregateGroupsAndSetsCorrectly() throws Exception {
    Job job = buildJobForTest();

    JobPayloads preview = await(job.composeOutputsPreview());

    Assert.assertEquals(10L, preview.getItemCount());
    Assert.assertNotNull(preview.getGroups());
    Assert.assertEquals(1, preview.getGroups().size());

    GroupPayloads group1 = preview.getGroups().get("group1");
    Assert.assertNotNull(group1);
    Assert.assertEquals(10L, group1.getItemCount());

    Map<String, SetPayloads> sets = group1.getSets();
    Assert.assertEquals(2, sets.size());
    Assert.assertEquals(5L, sets.get("setA").getItemCount());
    Assert.assertEquals(5L, sets.get("setB").getItemCount());

    Assert.assertEquals(0L, preview.getByteSize());
    Assert.assertEquals(0L, group1.getByteSize());
  }

  /**
   * Builds a job with one step in "group1" and one step with the specified (missing or empty) group, like the step that
   * delegates the user-facing outputs of an export.
   */
  private Job buildJobWithUngroupedStep(String groupOfUngroupedStep) {
    String jobId = "job-outputs-preview-ungrouped-test";
    OutputSetsTestStep groupedStep = new OutputSetsTestStep(jobId, "group1", Map.of("statistics", 1));
    OutputSetsTestStep ungroupedStep = new OutputSetsTestStep(jobId, groupOfUngroupedStep, Map.of("exportedData", 2));

    StepGraph graph = new StepGraph();
    graph.addExecution(groupedStep);
    graph.addExecution(ungroupedStep);

    Job job = new Job().withId(jobId).withSteps(graph);
    job.withStatus(new RuntimeStatus());
    return job;
  }

  @Test
  public void composeOutputsPreview_shouldExposeStepsWithoutGroupUnderDefaultGroup() throws Exception {
    Job job = buildJobWithUngroupedStep(null);

    JobPayloads preview = await(job.composeOutputsPreview());

    Assert.assertEquals(3L, preview.getItemCount());
    Assert.assertEquals(2, preview.getGroups().size());
    Assert.assertEquals(1L, preview.getGroups().get("group1").getItemCount());

    GroupPayloads defaultGroup = preview.getGroups().get(Step.InputSet.DEFAULT_SET_GROUP);
    Assert.assertNotNull(defaultGroup);
    Assert.assertEquals(2L, defaultGroup.getItemCount());
    Assert.assertEquals(2L, defaultGroup.getSets().get("exportedData").getItemCount());

    //A group without a name would not be serializable as a map key
    Assert.assertNotNull(preview.serialize());
  }

  @Test
  public void composeOutputsPreview_shouldTreatEmptyGroupLikeMissingGroup() throws Exception {
    JobPayloads preview = await(buildJobWithUngroupedStep("").composeOutputsPreview());

    Assert.assertEquals(2, preview.getGroups().size());
    Assert.assertEquals(2L, preview.getGroups().get(Step.InputSet.DEFAULT_SET_GROUP).getItemCount());
  }

  @Test
  public void defaultGroup_shouldBeAddressableForStepsWithoutGroup() throws Exception {
    Job job = buildJobWithUngroupedStep(null);

    GroupPayloads defaultGroup = await(job.composeOutputGroupPreview(Step.InputSet.DEFAULT_SET_GROUP));
    Assert.assertEquals(2L, defaultGroup.getItemCount());
    Assert.assertEquals(2L, defaultGroup.getSets().get("exportedData").getItemCount());
    Assert.assertNull(defaultGroup.getSets().get("statistics"));

    //The lookup used for paging through the items of a set
    Assert.assertNotNull(job.getSteps().getStepOrNull(Step.InputSet.DEFAULT_SET_GROUP, "exportedData"));
    Assert.assertNull(job.getSteps().getStepOrNull(Step.InputSet.DEFAULT_SET_GROUP, "statistics"));
    Assert.assertNotNull(job.getSteps().getStepOrNull("group1", "statistics"));
  }
}
