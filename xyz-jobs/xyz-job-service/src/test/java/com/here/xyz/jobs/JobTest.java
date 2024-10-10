package com.here.xyz.jobs;

import com.here.xyz.jobs.util.test.JobTestBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.io.IOException;

public class JobTest extends JobTestBase {
    @BeforeEach
    public void setUp() {
        createSpace(SPACE_ID);
    }

    @AfterEach
    public void tearDown() {
        deleteSpace(SPACE_ID);
    }

    protected void checkSucceededJob(Job exportJob) throws IOException, InterruptedException {
        RuntimeStatus status = getJobStatus(exportJob.getId());
        Assertions.assertEquals(RuntimeInfo.State.SUCCEEDED, status.getState());
        Assertions.assertEquals(status.getOverallStepCount(), status.getSucceededSteps());
    }
}
