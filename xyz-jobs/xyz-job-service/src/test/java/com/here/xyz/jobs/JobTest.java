package com.here.xyz.jobs;

import com.here.xyz.jobs.util.test.JobTestBase;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;

public class JobTest extends JobTestBase {

    @AfterEach
    public void tearDown() throws IOException, InterruptedException {
        cleanResources();
    }

    protected void checkSucceededJob(Job exportJob, int expectedFeatureCount) throws IOException, InterruptedException {
        checkRuntimeStatus(exportJob.getId());
        checkOutputs(exportJob, expectedFeatureCount);
    }

    protected void checkJobReusage(Job reusedJob, Job exportJob, int expectedFeatureCount, boolean expectMatch) throws IOException, InterruptedException {
        checkRuntimeStatus(exportJob.getId());
        checkOutputs(reusedJob, exportJob, expectedFeatureCount, expectMatch);
    }

    private void checkRuntimeStatus(String jobId) throws IOException, InterruptedException {
        RuntimeStatus status = getJobStatus(jobId);
        Assertions.assertEquals(RuntimeInfo.State.SUCCEEDED, status.getState());
        Assertions.assertEquals(status.getOverallStepCount(), status.getSucceededSteps());
    }

    private void checkOutputs(Job exportJob, int expectedFeatureCount) throws IOException, InterruptedException {
        checkOutputs(null, exportJob, expectedFeatureCount, false);
    }

    private void checkOutputs(Job firstJob, Job secondJob, int expectedFeatureCount, boolean expectMatch) throws IOException, InterruptedException {
        boolean foundStatistics = false;
        boolean foundUrls = false;

        List<Map> jobOutputs = getJobOutputs(secondJob.getId());
        for (Map jobOutput : jobOutputs) {
            if(jobOutput.get("type").equals("FeatureStatistics")){ //TODO: Use the FeatureStatistics model here instead
                foundStatistics = true;
                Assertions.assertEquals(expectedFeatureCount, jobOutput.get("featureCount"));
            }else if(jobOutput.get("type").equals("DownloadUrl")){
                foundUrls = true;
                if(expectMatch) {
                    //all links should point to firstJob (reused) Job
                    Assertions.assertTrue(((String) jobOutput.get("url")).contains(firstJob.getId()));
                    //no link should have a reference to the new job (secondJob)
                    Assertions.assertFalse(((String) jobOutput.get("url")).contains(secondJob.getId()));
                }else{
                    //all links should point to secondJob as reference
                    Assertions.assertTrue(((String) jobOutput.get("url")).contains(secondJob.getId()));
                    if(firstJob != null){
                        //no link should have a reference to the firstJob (noMatch)
                        Assertions.assertFalse(((String) jobOutput.get("url")).contains(firstJob.getId()));
                    }
                }
            }
        }

        Assertions.assertTrue(foundStatistics);
        Assertions.assertTrue(foundUrls);
    }
}
