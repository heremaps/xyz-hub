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

package com.here.xyz.jobs.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.jobs.RuntimeInfo;
import com.here.xyz.jobs.steps.execution.SFNInspector;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

class JobAdminApiEmrFailureTest {
  private static final String SFN_ERROR = "States.TaskFailed";
  private static final String EMR_ERROR_MESSAGE = "EMR Job step:job1.s_abcdef with run ID run1 failed.";
  private static final String THROTTLING_STATE_DETAILS = "Unable to push logs, please ensure logging destination is valid and "
      + "execution role has sufficient permissions. Error: \"An error occurred (ThrottlingException) when calling the "
      + "DescribeLogGroups operation (reached max retries: 4): Rate exceeded\".";

  @Test
  void appliesTheEmrErrorOfAThrottledJobRunAndMarksItRetryable() {
    RuntimeInfo status = new RuntimeInfo();

    boolean changed = JobAdminApi.applyFailureInformationIfMissing(status, emrTaskFailure(THROTTLING_STATE_DETAILS));

    assertTrue(changed);
    assertEquals(EMR_ERROR_MESSAGE, status.getErrorMessage());
    assertEquals(THROTTLING_STATE_DETAILS, status.getErrorCause());
    assertTrue(status.isFailedRetryable());
  }

  @Test
  void appliesTheEmrErrorOfAFailedJobRun() {
    RuntimeInfo status = new RuntimeInfo();
    String stateDetails = "ExitCode: 1. Error: Some error in the job";

    boolean changed = JobAdminApi.applyFailureInformationIfMissing(status, emrTaskFailure(stateDetails));

    assertTrue(changed);
    assertEquals(EMR_ERROR_MESSAGE, status.getErrorMessage());
    assertEquals(stateDetails, status.getErrorCause());
    assertFalse(status.isFailedRetryable());
  }

  @Test
  void appliesTheSfnErrorIfTheCauseIsNoEmrJobRun() {
    RuntimeInfo status = new RuntimeInfo();
    String cause = "Some failure of the task";

    boolean changed = JobAdminApi.applyFailureInformationIfMissing(status, new SFNInspector.StepFailureInfo("s_abcdef", SFN_ERROR, cause));

    assertTrue(changed);
    assertEquals(SFN_ERROR, status.getErrorMessage());
    assertEquals(cause, status.getErrorCause());
    assertFalse(status.isFailedRetryable());
  }

  @Test
  void keepsExistingErrorInformationButMarksAThrottledJobRunRetryable() {
    RuntimeInfo status = new RuntimeInfo()
        .withErrorMessage("Existing message")
        .withErrorCause("Existing cause");

    boolean changed = JobAdminApi.applyFailureInformationIfMissing(status, emrTaskFailure(THROTTLING_STATE_DETAILS));

    assertTrue(changed);
    assertEquals("Existing message", status.getErrorMessage());
    assertEquals("Existing cause", status.getErrorCause());
    assertTrue(status.isFailedRetryable());
  }

  @Test
  void truncatesLongStateDetails() {
    RuntimeInfo status = new RuntimeInfo();
    String stateDetails = "x".repeat(3_000);

    JobAdminApi.applyFailureInformationIfMissing(status, emrTaskFailure(stateDetails));

    assertTrue(status.getErrorCause().endsWith(" ..."));
    assertTrue(status.getErrorCause().length() < stateDetails.length());
  }

  /**
   * @return The failure of a startJobRun.sync task, of which the cause is the failed EMR job run
   */
  private static SFNInspector.StepFailureInfo emrTaskFailure(String stateDetails) {
    String cause = new JsonObject()
        .put("ApplicationId", "app-1")
        .put("Arn", "arn:aws:emr-serverless:us-east-1:000000000000:/applications/app-1/jobruns/run1")
        .put("Attempt", 1)
        .put("JobRunId", "run1")
        .put("Name", "step:job1.s_abcdef")
        .put("State", "FAILED")
        .put("StateDetails", stateDetails)
        .put("RetryPolicy", new JsonObject().put("MaxAttempts", 1))
        .put("Tags", new JsonObject().put("owner", "test"))
        .encode();
    return new SFNInspector.StepFailureInfo("s_abcdef", SFN_ERROR, cause);
  }
}
