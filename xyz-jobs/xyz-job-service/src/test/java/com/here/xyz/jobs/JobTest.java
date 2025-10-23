package com.here.xyz.jobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.util.test.JobTestBase;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;

public class JobTest extends JobTestBase {

  @AfterEach
  public void tearDown() throws IOException, InterruptedException {
    cleanResources();
  }

  protected void checkSucceededJob(Job exportJob, int expectedFeatureCount) throws IOException, InterruptedException {
    checkRuntimeStatus(exportJob.getId());
    checkOutputs(exportJob, expectedFeatureCount);
  }

  private void checkRuntimeStatus(String jobId) throws IOException, InterruptedException {
    RuntimeStatus status = getJobStatus(jobId);
    assertEquals(RuntimeInfo.State.SUCCEEDED, status.getState());
    assertEquals(status.getOverallStepCount(), status.getSucceededSteps());
  }

  private void checkOutputs(Job exportJob, int expectedFeatureCount) throws IOException, InterruptedException {
    checkOutputs(null, exportJob, expectedFeatureCount, false);
  }

  private void checkOutputs(Job firstJob, Job secondJob, int expectedFeatureCount, boolean expectMatch)
      throws IOException, InterruptedException {
    boolean foundStatistics = false;
    boolean foundUrls = false;

    List<Map> jobOutputs = getJobOutputs(secondJob.getId());
    for (Map jobOutput : jobOutputs) {
      if (jobOutput.get("type").equals("FeatureStatistics")) {
        foundStatistics = true;
        assertEquals(expectedFeatureCount, jobOutput.get("featureCount"));
        assertEquals(((DatasetDescription.Space)(secondJob.getSource())).getVersionRef().toString(), jobOutput.get("versionRef"));
      }
      else if (jobOutput.get("type").equals("DownloadUrl")) {
        foundUrls = true;
        if (expectMatch) {
          //all links should point to firstJob (reused) Job
          assertTrue(((String) jobOutput.get("url")).contains(firstJob.getId()));
          //no link should have a reference to the new job (secondJob)
          assertFalse(((String) jobOutput.get("url")).contains(secondJob.getId()));
        }
        else {
          //all links should point to secondJob as reference
          assertTrue(((String) jobOutput.get("url")).contains(secondJob.getId()));
          if (firstJob != null) {
            //no link should have a reference to the firstJob (noMatch)
            assertFalse(((String) jobOutput.get("url")).contains(firstJob.getId()));
          }
        }
      }
    }

    assertTrue(foundStatistics);
    if(expectedFeatureCount != 0)
      assertTrue(foundUrls);
  }

  protected void checkJobReusage(Job reusedJob, Job exportJob, int expectedFeatureCount, boolean expectMatch)
      throws IOException, InterruptedException {
    checkRuntimeStatus(exportJob.getId());
    checkOutputs(reusedJob, exportJob, expectedFeatureCount, expectMatch);
  }
}
