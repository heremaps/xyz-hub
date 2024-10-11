package com.here.xyz.jobs;

import com.here.xyz.jobs.util.test.JobTestBase;
import org.junit.jupiter.api.Assertions;

import java.io.IOException;

public class JobTest extends JobTestBase {

    protected void checkSucceededJob(Job exportJob) throws IOException, InterruptedException {
        RuntimeStatus status = getJobStatus(exportJob.getId());
        Assertions.assertEquals(RuntimeInfo.State.SUCCEEDED, status.getState());
        Assertions.assertEquals(status.getOverallStepCount(), status.getSucceededSteps());
    }
}
