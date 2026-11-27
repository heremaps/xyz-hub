package com.here.xyz.jobs;

import static com.here.xyz.jobs.RuntimeInfo.State.FAILED;
import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;
import static com.here.xyz.jobs.RuntimeInfo.State.RESUMING;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.here.xyz.jobs.steps.StepGraph;
import com.here.xyz.jobs.steps.execution.fusion.OutputSetsTestStep;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class JobResumeTest {

  @Test
  public void resumedStepsBecomePendingAndAcceptLaterStateUpdates() {
    OutputSetsTestStep failed = new OutputSetsTestStep("job-resume-test", "group1", Map.of());
    OutputSetsTestStep succeeded = new OutputSetsTestStep("job-resume-test", "group1", Map.of());
    failed.getStatus().setState(FAILED);
    succeeded.getStatus().setState(SUCCEEDED);
    Job job = new Job().withId("job-resume-test").withSteps(new StepGraph().addExecution(failed).addExecution(succeeded));

    job.setStateOfResumableSteps(RESUMING);
    assertEquals(RESUMING, failed.getStatus().getState());
    assertEquals(SUCCEEDED, succeeded.getStatus().getState());

    job.setStateOfResumableSteps(PENDING);
    assertEquals(PENDING, failed.getStatus().getState());
    assertEquals(SUCCEEDED, succeeded.getStatus().getState());

    //E.g. the RUNNING and SUCCESS state change events of the re-executed EMR job
    assertDoesNotThrow(() -> failed.getStatus().setState(RUNNING));
    assertDoesNotThrow(() -> failed.getStatus().setState(SUCCEEDED));
  }
}
