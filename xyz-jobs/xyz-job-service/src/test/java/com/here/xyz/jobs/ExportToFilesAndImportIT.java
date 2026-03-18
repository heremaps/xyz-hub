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
package com.here.xyz.jobs;

import com.here.xyz.jobs.datasets.DatasetDescription;
import com.here.xyz.jobs.processes.CopyViaFiles;
import com.here.xyz.models.hub.Ref;
import com.here.xyz.models.hub.Space;
import com.here.xyz.models.hub.Tag;
import com.here.xyz.responses.StatisticsResponse;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Integration tests for the {@link com.here.xyz.jobs.steps.compiler.ExportToFilesAndImport} compiler.
 * Verifies that data can be exported from a source space to files and then imported into a target space
 * using the CopyViaFiles process description.
 */
public class ExportToFilesAndImportIT extends JobTest {

/* Jobconfiguration pattern :
 {
  "description": "ExportToFilesAndImport Test",
  "process": { "type": "CopyViaFiles" },
  "source": { "type": "Space", "id": "testExpImp-Source-01" },
  "target": { "type": "Space", "id": "testExpImp-Target-01" }
 }
*/

  private static final String SRC_SPACE = "testExpImp-Source-01";
  private static final String TRG_SPACE = "testExpImp-Target-01";

  @BeforeEach
  public void setup() throws SQLException {
    cleanup();
    createSpace(new Space().withId(SRC_SPACE).withVersionsToKeep(100), false);
    createSpace(new Space().withId(TRG_SPACE).withVersionsToKeep(100), false);
  }

  @AfterEach
  public void cleanup() throws SQLException {
    deleteSpace(SRC_SPACE);
    deleteSpace(TRG_SPACE);
  }

  private void checkSucceededJob(Job job) throws IOException, InterruptedException {
    RuntimeStatus status = getJobStatus(job.getId());
    Assertions.assertEquals(RuntimeInfo.State.SUCCEEDED, status.getState());
    Assertions.assertEquals(status.getOverallStepCount(), status.getSucceededSteps());
  }

  private Job buildCopyViaFilesJob(String versionRef) {
    return new Job()
        .withId(JOB_ID)
        .withDescription("ExportToFilesAndImport Test")
        .withProcess(new CopyViaFiles())
        .withSource(new DatasetDescription.Space<>().withId(SRC_SPACE)
            .withVersionRef(versionRef == null ? new Ref("HEAD") : new Ref(versionRef)))
        .withTarget(new DatasetDescription.Space<>().withId(TRG_SPACE));
  }

  /**
   * Test copying all features from a non-empty source space into an empty target space.
   */
  @Test
  public void testCopyViaFiles_allFeatures_intoEmptyTarget() throws Exception {
    putRandomFeatureCollectionToSpace(SRC_SPACE, 30);

    Job job = buildCopyViaFilesJob(null);
    createSelfRunningJob(job);
    checkSucceededJob(job);

    List<Map> outputs = getJobOutputs(job.getId());
    Assertions.assertEquals(3, outputs.size());
    Assertions.assertEquals(30, outputs.get(0).get("featureCount"));

    // Verify that the target space actually contains the features
    StatisticsResponse targetStats = getStatistics(TRG_SPACE);
    Assertions.assertNotNull(targetStats);
    Assertions.assertEquals(30L, targetStats.getCount().getValue());
  }

  /**
   * Test copying all features from a non-empty source space into a non-empty target space.
   * Features are appended / merged into the target.
   */
  @Test
  public void testCopyViaFiles_allFeatures_intoNonEmptyTarget() throws Exception {
    putRandomFeatureCollectionToSpace(SRC_SPACE, 20);
    putRandomFeatureCollectionToSpace(TRG_SPACE, 5);

    Job job = buildCopyViaFilesJob(null);
    createSelfRunningJob(job);
    checkSucceededJob(job);

    List<Map> outputs = getJobOutputs(job.getId());
    Assertions.assertEquals(3, outputs.size());
    Assertions.assertEquals(20, outputs.get(0).get("featureCount"));

    // Target should contain the 5 original + 20 imported features
    StatisticsResponse targetStats = getStatistics(TRG_SPACE);
    Assertions.assertNotNull(targetStats);
    Assertions.assertEquals(25L, targetStats.getCount().getValue());
  }

  /**
   * Test copying features for a specific version range using a tag reference.
   */
  @Test
  public void testCopyViaFiles_withVersionRef() throws Exception {
    String tagId = "tagV1";

    putRandomFeatureCollectionToSpace(SRC_SPACE, 15); // v1
    createTag(SRC_SPACE, new Tag().withId(tagId).withVersion(1));
    putRandomFeatureCollectionToSpace(SRC_SPACE, 10); // v2 - additional features after tag

    Job job = buildCopyViaFilesJob(tagId);
    createSelfRunningJob(job);
    checkSucceededJob(job);

    List<Map> outputs = getJobOutputs(job.getId());
    Assertions.assertEquals(3, outputs.size());
    // Only the features up to the tagged version should be exported
    Assertions.assertEquals(15, outputs.get(0).get("featureCount"));

    StatisticsResponse targetStats = getStatistics(TRG_SPACE);
    Assertions.assertNotNull(targetStats);
    Assertions.assertEquals(15L, targetStats.getCount().getValue());

    deleteTag(SRC_SPACE, tagId);
  }

  /**
   * Test copying from an empty source space - job should succeed with zero features.
   */
  @Test
  public void testCopyViaFiles_emptySourceSpace() throws Exception {
    Job job = buildCopyViaFilesJob(null);
    createSelfRunningJob(job);
    checkSucceededJob(job);

    List<Map> outputs = getJobOutputs(job.getId());
    Assertions.assertEquals(1, outputs.size());
    Assertions.assertEquals(0, outputs.get(0).get("featureCount"));

    StatisticsResponse targetStats = getStatistics(TRG_SPACE);
    Assertions.assertNotNull(targetStats);
    Assertions.assertEquals(0L, targetStats.getCount().getValue());
  }
}
