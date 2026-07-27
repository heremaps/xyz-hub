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
import com.here.xyz.jobs.steps.impl.StepTest;
import com.here.xyz.jobs.steps.inputs.Input;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Runs against localstack, which has no S3 Batch Operations. There {@link S3BatchOperations#scheduleForDeletion} writes the manifest but,
 * instead of submitting a CreateJob, deletes the job's objects directly and then removes the manifest (no bucket lifecycle rule exists
 * locally to expire it). Each test asserts the right objects are removed and the manifest is cleaned up.
 */
public class S3BatchOperationsTest extends StepTest {

  private static final String manifestPrefix = "_batch-manifests/";

  private void writeInputMetadata(Set<String> referencingJobs) throws IOException {
    Input.InputsMetadata meta = new Input.InputsMetadata(Map.of(), referencingJobs, null, null);
    uploadFileToS3(JOB_ID + "/meta/" + DEFAULT_SET_NAME + ".json", S3ContentType.APPLICATION_JSON,
        XyzSerializable.serialize(meta).getBytes(), false);
  }

  private void assertManifestCleanedUp() {
    assertFalse(S3Client.getInstance().isFolder(manifestPrefix + JOB_ID + "/"),
        "the manifest should have been deleted locally after use");
  }

  @Test
  public void schedulingAPrefixDeletesObjectsAndManifest() throws IOException {
    uploadInputFile(JOB_ID, "input-a".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadInputFile(JOB_ID, "input-b".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadOutputFile(JOB_ID, "s_1", "outputSet", "output-a".getBytes(), S3ContentType.APPLICATION_JSON);

    List<String> prefixes = List.of(JOB_ID + "/");
    assertEquals(3, S3Client.getInstance().listObjects(JOB_ID + "/").size(), "test setup should have created 3 objects");

    Optional<String> batchJobId = S3BatchOperations.scheduleForDeletion(JOB_ID, prefixes);
    assertTrue(batchJobId.isEmpty(), "S3 Batch Operations CreateJob is skipped when running locally");

    assertTrue(S3Client.getInstance().listObjects(JOB_ID + "/").isEmpty(),
        "every object under the prefix should have been deleted locally");
    assertManifestCleanedUp();
  }

  @Test
  public void scheduleForDeletionWithNoObjectsDoesNothing() {
    Optional<String> batchJobId = S3BatchOperations.scheduleForDeletion(JOB_ID, List.of(JOB_ID + "/"));

    assertTrue(batchJobId.isEmpty(), "no batch job should be created when there is nothing to schedule");
    assertFalse(S3Client.getInstance().isFolder(manifestPrefix + JOB_ID + "/"),
        "no manifest should be written when there are no objects");
    assertFalse(S3Client.getInstance().isFolder(JOB_ID + "/"), "nothing should exist for the job");
  }

  @Test
  public void jobInputsAreDeleted() throws IOException {
    uploadInputFile(JOB_ID, "feature-a".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadInputFile(JOB_ID, "feature-b".getBytes(), S3ContentType.APPLICATION_JSON);
    writeInputMetadata(Set.of(JOB_ID));

    List<String> prefixes = Input.collectInputPrefixesForDeletion(JOB_ID);
    assertEquals(List.of(inputS3Prefix(JOB_ID, DEFAULT_SET_NAME)), prefixes,
        "the job's own input set should be eligible for deletion");

    S3BatchOperations.scheduleForDeletion(JOB_ID, prefixes);

    assertTrue(S3Client.getInstance().listObjects(inputS3Prefix(JOB_ID, DEFAULT_SET_NAME)).isEmpty(),
        "the job's inputs should have been deleted");
    assertManifestCleanedUp();
  }

  @Test
  public void inputsWithoutMetadataAreStillDeleted() throws IOException {
    uploadInputFile(JOB_ID, "orphan-input".getBytes(), S3ContentType.APPLICATION_JSON);

    List<String> prefixes = Input.collectInputPrefixesForDeletion(JOB_ID);
    assertEquals(List.of(inputS3Prefix(JOB_ID)), prefixes,
        "with no input metadata, the whole inputs prefix should be deleted");

    S3BatchOperations.scheduleForDeletion(JOB_ID, prefixes);

    assertTrue(S3Client.getInstance().listObjects(inputS3Prefix(JOB_ID)).isEmpty(),
        "the orphan input (no metadata) should still be deleted");
    assertManifestCleanedUp();
  }

  @Test
  public void inputsStillReferencedByAnotherJobAreNotDeleted() throws IOException {
    uploadInputFile(JOB_ID, "shared-input".getBytes(), S3ContentType.APPLICATION_JSON);
    writeInputMetadata(Set.of(JOB_ID, "some-other-live-job"));

    List<String> prefixes = Input.collectInputPrefixesForDeletion(JOB_ID);
    assertTrue(prefixes.isEmpty(), "an input set still referenced by another job must be skipped");

    S3BatchOperations.scheduleForDeletion(JOB_ID, prefixes);
    assertFalse(S3Client.getInstance().listObjects(inputS3Prefix(JOB_ID, DEFAULT_SET_NAME)).isEmpty(),
        "the shared input must still be present");
  }

  @Test
  public void stepOutputsAreDeleted() throws IOException {
    String stepId = "s_export";
    uploadOutputFile(JOB_ID, stepId, "exportedData", "out-a".getBytes(), S3ContentType.APPLICATION_JSON);
    uploadOutputFile(JOB_ID, stepId, "exportedData", "out-b".getBytes(), S3ContentType.APPLICATION_JSON);

    List<String> prefixes = List.of(JOB_ID + "/" + stepId); //matches Step.getOutputS3Prefix()
    assertEquals(2, S3Client.getInstance().listObjects(prefixes.get(0)).size(), "the two step outputs should be present");

    S3BatchOperations.scheduleForDeletion(JOB_ID, prefixes);

    assertTrue(S3Client.getInstance().listObjects(JOB_ID + "/" + stepId).isEmpty(),
        "the step outputs should have been deleted");
    assertManifestCleanedUp();
  }
}
