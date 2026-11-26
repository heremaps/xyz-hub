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

package com.here.xyz.jobs.steps.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

public class EmrJobRunFailureTest {
  private static final String THROTTLING_STATE_DETAILS = "Unable to push logs, please ensure logging destination is valid and "
      + "execution role has sufficient permissions. Error: \"An error occurred (ThrottlingException) when calling the "
      + "DescribeLogGroups operation (reached max retries: 4): Rate exceeded\".";

  @Test
  public void parsesFailedJobRun() {
    EmrJobRunFailure failure = EmrJobRunFailure.fromSfnCause(jobRun("FAILED", THROTTLING_STATE_DETAILS).encode());

    assertEquals(new EmrJobRunFailure("step:job1.s_abcdef", "run1", THROTTLING_STATE_DETAILS), failure);
    assertEquals("EMR Job step:job1.s_abcdef with run ID run1 failed.", failure.errorMessage());
    assertTrue(failure.isTransient());
  }

  @Test
  public void parsesFailedJobRunWithoutStateDetails() {
    JsonObject jobRun = jobRun("FAILED", null);
    jobRun.remove("StateDetails");

    EmrJobRunFailure failure = EmrJobRunFailure.fromSfnCause(jobRun.encodePrettily());

    assertEquals(new EmrJobRunFailure("step:job1.s_abcdef", "run1", null), failure);
    assertFalse(failure.isTransient());
  }

  @Test
  public void otherCausesAreNoEmrJobRunFailure() {
    assertNull(EmrJobRunFailure.fromSfnCause(null));
    assertNull(EmrJobRunFailure.fromSfnCause(""));
    assertNull(EmrJobRunFailure.fromSfnCause("Some error"));
    assertNull(EmrJobRunFailure.fromSfnCause("{not JSON"));
    assertNull(EmrJobRunFailure.fromSfnCause("{\"errorMessage\": \"Some error\"}"));

    JsonObject withoutJobRunId = jobRun("FAILED", THROTTLING_STATE_DETAILS);
    withoutJobRunId.remove("JobRunId");
    assertNull(EmrJobRunFailure.fromSfnCause(withoutJobRunId.encode()));

    JsonObject withBlankName = jobRun("FAILED", THROTTLING_STATE_DETAILS).put("Name", " ");
    assertNull(EmrJobRunFailure.fromSfnCause(withBlankName.encode()));

    assertNull(EmrJobRunFailure.fromSfnCause(jobRun("CANCELLED", "Job run was cancelled.").encode()));
  }

  @Test
  public void throttlingIsTransient() {
    assertTrue(EmrJobRunFailure.isTransient(THROTTLING_STATE_DETAILS));
    assertTrue(EmrJobRunFailure.isTransient("Rate exceeded"));
    assertFalse(EmrJobRunFailure.isTransient("ExitCode: 1. Error: Some error in the job"));
    assertFalse(EmrJobRunFailure.isTransient(null));
  }

  @Test
  public void errorMessage() {
    assertEquals("EMR Job step:job1.s_abcdef with run ID run1 failed.", EmrJobRunFailure.errorMessage("step:job1.s_abcdef", "run1"));
  }

  /**
   * @return A job run as it is reported in the cause of a failed startJobRun.sync task
   */
  static JsonObject jobRun(String state, String stateDetails) {
    return new JsonObject()
        .put("ApplicationId", "app-1")
        .put("Arn", "arn:aws:emr-serverless:us-east-1:000000000000:/applications/app-1/jobruns/run1")
        .put("Attempt", 1)
        .put("JobRunId", "run1")
        .put("Name", "step:job1.s_abcdef")
        .put("State", state)
        .put("StateDetails", stateDetails)
        .put("RetryPolicy", new JsonObject().put("MaxAttempts", 1))
        .put("Tags", new JsonObject().put("owner", "test"));
  }
}
