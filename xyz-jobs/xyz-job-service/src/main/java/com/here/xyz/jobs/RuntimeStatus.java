/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static com.here.xyz.jobs.RuntimeInfo.State.PENDING;

import com.fasterxml.jackson.annotation.JsonView;

public class RuntimeStatus extends RuntimeInfo<RuntimeStatus> {
  private Action desiredAction;
  private int overallStepCount;
  private int succeededSteps;
  @JsonView({Public.class, Static.class})
  private long estimatedStartTime = -1;

  /**
   * The desired action can be set by the user to define the intent of executing some action on the status of
   * the job. If the action could not be executed, the value stays as it was defined by the user.
   * Once the execution has been successfully performed, that value will be unset.
   * @return
   */
  public Action getDesiredAction() {
    return desiredAction;
  }

  public void setDesiredAction(Action desiredAction) {
    this.desiredAction = desiredAction;
  }

  public RuntimeStatus withDesiredAction(Action desiredAction) {
    setDesiredAction(desiredAction);
    return this;
  }

  public int getOverallStepCount() {
    return overallStepCount;
  }

  public void setOverallStepCount(int overallStepCount) {
    this.overallStepCount = overallStepCount;
  }

  public RuntimeStatus withOverallStepCount(int overallStepCount) {
    setOverallStepCount(overallStepCount);
    return this;
  }

  public int getSucceededSteps() {
    return succeededSteps;
  }

  public void setSucceededSteps(int succeededSteps) {
    touch();
    this.succeededSteps = succeededSteps;
  }

  public RuntimeStatus withSucceededSteps(int succeededSteps) {
    setSucceededSteps(succeededSteps);
    return this;
  }

  public long getEstimatedStartTime() {
    if (getState() != PENDING)
      return -1;
    return estimatedStartTime;
  }

  public void setEstimatedStartTime(long estimatedStartTime) {
    this.estimatedStartTime = estimatedStartTime;
  }

  public RuntimeStatus withEstimatedStartTime(long estimatedStartTime) {
    setEstimatedStartTime(estimatedStartTime);
    return this;
  }

  public enum Action {
    START,
    CANCEL,
    RESUME
  }
}
