/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.jobs.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.impl.StepTest;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class S3BatchOperationsTest extends StepTest {

  @BeforeAll
  public static void setupBatchRole() {
    Config.instance.S3_BATCH_OPS_ROLE_ARN = "arn:aws:iam::000000000000:role/test-batch-ops";
  }

  @Test
  public void scheduleForDeletionWritesManifestOfAllJobObjects() throws IOException {
    uploadInputFile(JOB_ID, "input-a".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadInputFile(JOB_ID, "input-b".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadOutputFile(JOB_ID, "s_1", "outputSet", "output-a".getBytes(), S3ContentType.APPLICATION_JSON);

    List<String> expectedKeys = S3Client.getInstance().listObjects(JOB_ID + "/");
    assertEquals(3, expectedKeys.size(), "test setup should have created 3 objects under the job prefix");

    Optional<String> batchJobId = S3BatchOperations.scheduleForDeletion(JOB_ID, List.of(JOB_ID + "/"));

    assertTrue(batchJobId.isEmpty(), "S3 Batch Operations CreateJob is skipped when running locally");

    List<String> manifestKeys = S3Client.getInstance().listObjects("_batch-manifests/" + JOB_ID + "/");
    assertEquals(1, manifestKeys.size(), "exactly one manifest should have been written for the job");

    String manifest = new String(S3Client.getInstance().loadObjectContent(manifestKeys.get(0)));
    Set<String> manifestRows = Set.of(manifest.split("\n"));

    Set<String> expectedRows = expectedKeys.stream()
        .map(key -> Config.instance.JOBS_S3_BUCKET + "," + key)
        .collect(Collectors.toSet());
    assertEquals(expectedRows, manifestRows, "manifest should list every job object exactly once as bucket,key");

    cleanS3Files("_batch-manifests/" + JOB_ID + "/");
  }

  @Test
  public void scheduleForDeletionWithNoObjectsDoesNothing() {
    Optional<String> batchJobId = S3BatchOperations.scheduleForDeletion(JOB_ID, List.of(JOB_ID + "/"));

    assertTrue(batchJobId.isEmpty(), "no batch job should be created when there is nothing to schedule");
    assertFalse(S3Client.getInstance().isFolder("_batch-manifests/" + JOB_ID + "/"),
        "no manifest should be written when there are no objects");
  }
}
