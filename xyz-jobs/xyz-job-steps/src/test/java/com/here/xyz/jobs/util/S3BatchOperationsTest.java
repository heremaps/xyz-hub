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

import static com.here.xyz.jobs.steps.Step.InputSet.DEFAULT_SET_NAME;
import static com.here.xyz.jobs.steps.inputs.Input.inputS3Prefix;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.jobs.steps.Config;
import com.here.xyz.jobs.steps.impl.StepTest;
import com.here.xyz.jobs.steps.inputs.Input;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class S3BatchOperationsTest extends StepTest {

  @BeforeAll
  public static void setupBatchRole() {
    Config.instance.S3_BATCH_OPS_ROLE_ARN = "arn:aws:iam::000000000000:role/test-batch-ops";
  }

  @AfterEach
  public void cleanupManifests() {
    cleanS3Files("_batch-manifests/" + JOB_ID + "/");
  }

  private void writeInputMetadata(Set<String> referencingJobs) throws IOException {
    Input.InputsMetadata meta = new Input.InputsMetadata(Map.of(), referencingJobs, null, null);
    uploadFileToS3(JOB_ID + "/meta/" + DEFAULT_SET_NAME + ".json", S3ContentType.APPLICATION_JSON,
        XyzSerializable.serialize(meta).getBytes(), false);
  }

  private String readSingleManifest() throws IOException {
    List<String> manifestKeys = S3Client.getInstance().listObjects("_batch-manifests/" + JOB_ID + "/");
    assertEquals(1, manifestKeys.size(), "exactly one manifest should have been written for the job");
    return new String(S3Client.getInstance().loadObjectContent(manifestKeys.get(0)));
  }

  private Set<String> manifestRowsFor(List<String> prefixes) {
    return prefixes.stream()
        .flatMap(prefix -> S3Client.getInstance().listObjects(prefix).stream())
        .map(key -> Config.instance.JOBS_S3_BUCKET + "," + key)
        .collect(Collectors.toSet());
  }

  @Test
  public void schedulingAPrefixWritesAManifestOfAllItsObjects() throws IOException {
    uploadInputFile(JOB_ID, "input-a".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadInputFile(JOB_ID, "input-b".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadOutputFile(JOB_ID, "s_1", "outputSet", "output-a".getBytes(), S3ContentType.APPLICATION_JSON);

    List<String> prefixes = List.of(JOB_ID + "/");
    assertEquals(3, S3Client.getInstance().listObjects(JOB_ID + "/").size(), "test setup should have created 3 objects");

    Optional<String> batchJobId = S3BatchOperations.scheduleForDeletion(JOB_ID, prefixes);
    assertTrue(batchJobId.isEmpty(), "S3 Batch Operations CreateJob is skipped when running locally");

    assertEquals(manifestRowsFor(prefixes), Set.of(readSingleManifest().split("\n")),
        "manifest should list every object under the prefix exactly once as bucket,key");
  }

  @Test
  public void scheduleForDeletionWithNoObjectsDoesNothing() {
    Optional<String> batchJobId = S3BatchOperations.scheduleForDeletion(JOB_ID, List.of(JOB_ID + "/"));

    assertTrue(batchJobId.isEmpty(), "no batch job should be created when there is nothing to schedule");
    assertFalse(S3Client.getInstance().isFolder("_batch-manifests/" + JOB_ID + "/"),
        "no manifest should be written when there are no objects");
  }

  @Test
  public void jobInputsAreScheduledForDeletion() throws IOException {
    uploadInputFile(JOB_ID, "feature-a".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadInputFile(JOB_ID, "feature-b".getBytes(), S3ContentType.APPLICATION_JSON);
    writeInputMetadata(Set.of(JOB_ID));

    List<String> prefixes = Input.collectInputPrefixesForDeletion(JOB_ID);
    assertEquals(List.of(inputS3Prefix(JOB_ID, DEFAULT_SET_NAME)), prefixes,
        "the job's own input set should be eligible for deletion");

    Optional<String> batchJobId = S3BatchOperations.scheduleForDeletion(JOB_ID, prefixes);
    assertTrue(batchJobId.isEmpty(), "S3 Batch Operations CreateJob is skipped when running locally");

    assertEquals(manifestRowsFor(prefixes), Set.of(readSingleManifest().split("\n")),
        "manifest should list every input object as bucket,key");
  }

  @Test
  public void inputsWithoutMetadataAreStillScheduled() throws IOException {
    uploadInputFile(JOB_ID, "orphan-input".getBytes(), S3ContentType.APPLICATION_JSON);

    List<String> prefixes = Input.collectInputPrefixesForDeletion(JOB_ID);
    assertEquals(List.of(inputS3Prefix(JOB_ID)), prefixes,
        "with no input metadata, the whole inputs prefix should be scheduled");

    S3BatchOperations.scheduleForDeletion(JOB_ID, prefixes);
    assertEquals(1, readSingleManifest().split("\n").length, "the orphan input (no metadata) should still be scheduled");
  }

  @Test
  public void inputsStillReferencedByAnotherJobAreNotScheduled() throws IOException {
    uploadInputFile(JOB_ID, "shared-input".getBytes(), S3ContentType.APPLICATION_JSON);
    writeInputMetadata(Set.of(JOB_ID, "some-other-live-job"));

    assertTrue(Input.collectInputPrefixesForDeletion(JOB_ID).isEmpty(),
        "an input set still referenced by another job must be skipped");
  }

  @Test
  public void stepOutputsAreScheduledForDeletion() throws IOException {
    String stepId = "s_export";
    uploadOutputFile(JOB_ID, stepId, "exportedData", "out-a".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadOutputFile(JOB_ID, stepId, "exportedData", "out-b".getBytes(), S3ContentType.APPLICATION_JSON);

    List<String> prefixes = List.of(JOB_ID + "/" + stepId); //matches Step.getOutputS3Prefix()
    assertEquals(2, S3Client.getInstance().listObjects(prefixes.get(0)).size(), "the two step outputs should be present");

    S3BatchOperations.scheduleForDeletion(JOB_ID, prefixes);
    assertEquals(manifestRowsFor(prefixes), Set.of(readSingleManifest().split("\n")),
        "manifest should list every step output as bucket,key");
  }
}
