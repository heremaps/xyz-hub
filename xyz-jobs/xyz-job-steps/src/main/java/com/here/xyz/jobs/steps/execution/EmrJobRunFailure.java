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

import io.vertx.core.json.JsonObject;

/**
 * The failure of an EMR job run as reported by the failed startJobRun.sync task of a state machine.
 * The cause of that task failure is the job run as JSON with PascalCase keys, e.g.
 * <code>{"JobRunId": "...", "Name": "...", "State": "FAILED", "StateDetails": "..."}</code>.
 *
 * @param name The name of the job run
 * @param jobRunId The ID of the job run
 * @param stateDetails The details EMR provides about the failure or <code>null</code>
 */
public record EmrJobRunFailure(String name, String jobRunId, String stateDetails) {

  /**
   * @param sfnCause The cause of a failed task of a state machine
   * @return The failure of the EMR job run or <code>null</code> if the cause is not a failed EMR job run
   */
  public static EmrJobRunFailure fromSfnCause(String sfnCause) {
    if (sfnCause == null || !sfnCause.trim().startsWith("{"))
      return null;

    try {
      JsonObject jobRun = new JsonObject(sfnCause.trim());
      if (!"FAILED".equals(jobRun.getValue("State")))
        return null;
      if (!(jobRun.getValue("Name") instanceof String name) || name.isBlank())
        return null;
      if (!(jobRun.getValue("JobRunId") instanceof String jobRunId) || jobRunId.isBlank())
        return null;
      return new EmrJobRunFailure(name, jobRunId, jobRun.getValue("StateDetails") instanceof String stateDetails ? stateDetails : null);
    }
    catch (RuntimeException e) {
      return null;
    }
  }

  public static String errorMessage(String jobRunName, String jobRunId) {
    return "EMR Job " + jobRunName + " with run ID " + jobRunId + " failed.";
  }

  public String errorMessage() {
    return errorMessage(name, jobRunId);
  }

  /**
   * @param stateDetails The state details of a failed EMR job run
   * @return Whether the job run failed because of a transient issue, so that running it again may succeed
   */
  public static boolean isTransient(String stateDetails) {
    //e.g. throttled calls of EMR to the CloudWatch Logs API while it sets up the logging of the job run
    return stateDetails != null && (stateDetails.contains("ThrottlingException") || stateDetails.contains("Rate exceeded"));
  }

  public boolean isTransient() {
    return isTransient(stateDetails);
  }
}
