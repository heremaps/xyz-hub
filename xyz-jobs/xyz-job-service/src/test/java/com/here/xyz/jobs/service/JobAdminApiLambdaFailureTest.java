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
import org.junit.jupiter.api.Test;

class JobAdminApiLambdaFailureTest {

  @Test
  void appliesCodedSfnCauseWhenLambdaStatusSynchronizationWasMissing() {
    RuntimeInfo status = new RuntimeInfo();
    SFNInspector.StepFailureInfo failureInfo = new SFNInspector.StepFailureInfo(
        "s_ylloyr",
        "MicroBatchExecutionFailedException",
        "wrapper\\nCaused by: ConfigurationParseException: E316052 Cannot parse configuration.\\nnext");

    boolean changed = JobAdminApi.applyFailureInformationIfMissing(status, failureInfo);

    assertTrue(changed);
    assertEquals("MicroBatchExecutionFailedException", status.getErrorMessage());
    assertEquals("E316052 Cannot parse configuration.", status.getErrorCause());
  }

  @Test
  void preservesErrorInformationAlreadySynchronizedByLambda() {
    RuntimeInfo status = new RuntimeInfo()
        .withErrorMessage("Failed to execute MPA")
        .withErrorCause("E316052 Cannot parse configuration.");
    SFNInspector.StepFailureInfo failureInfo = new SFNInspector.StepFailureInfo(
        "s_ylloyr", "Lambda.Unknown", "generic Step Functions cause");

    boolean changed = JobAdminApi.applyFailureInformationIfMissing(status, failureInfo);

    assertFalse(changed);
    assertEquals("Failed to execute MPA", status.getErrorMessage());
    assertEquals("E316052 Cannot parse configuration.", status.getErrorCause());
  }
}
