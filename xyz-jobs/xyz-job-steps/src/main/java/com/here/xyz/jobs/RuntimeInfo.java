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

import static com.here.xyz.jobs.RuntimeInfo.State.NONE;
import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.RuntimeInfo.State.SUCCEEDED;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonView;
import com.google.common.collect.ImmutableMap;
import com.here.xyz.XyzSerializable;
import java.util.Arrays;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RuntimeInfo<T extends RuntimeInfo> implements XyzSerializable {
  @JsonView({Public.class, Static.class})
  private long updatedAt;
  @JsonView({Public.class, Static.class})
  private long startedAt;
  @JsonView({Public.class, Static.class})
  private State state = NONE;
  @JsonView({Public.class, Static.class})
  private float estimatedProgress;
  @JsonView({Public.class, Static.class})
  private String errorMessage;
  @JsonView({Public.class, Static.class})
  private String errorCause;
  @JsonView({Public.class, Static.class})
  private String errorCode;
  @JsonView({Public.class, Static.class})
  boolean failedRetryable;
  private long initialEndTimeEstimation = -1;

  /**
   * Updates the updatedAt timestamp of this object to the current time.
   */
  public void touch() {
    setUpdatedAt(System.currentTimeMillis());
  }

  /**
   * @return The estimated timestamp of when the process will be completed in milliseconds.
   */
  public long getEstimatedEndTime() {
    if (getState().isFinal())
      return getUpdatedAt();

    long passedExecutionTime = getUpdatedAt() - getStartedAt();
    float estimatedProgress = getEstimatedProgress();
    if (estimatedProgress == 0)
        return initialEndTimeEstimation;
    long estimatedOverallDuration = (long) (passedExecutionTime / estimatedProgress);
    return getStartedAt() + estimatedOverallDuration;
  }

  public T withInitialEndTimeEstimation(long initialEndTimeEstimation) {
    this.initialEndTimeEstimation = initialEndTimeEstimation;
    return (T) this;
  }

  /**
   * @return The timestamp of the last status update in milliseconds.
   */
  public long getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(long updatedAt) {
    this.updatedAt = updatedAt;
  }

  public T withUpdatedAt(long updatedAt) {
    setUpdatedAt(updatedAt);
    return (T) this;
  }

  public long getStartedAt() {
    return startedAt;
  }

  public void setStartedAt(long startedAt) {
    this.startedAt = startedAt;
  }

  public T withStartedAt(long startedAt) {
    setStartedAt(startedAt);
    return (T) this;
  }

  public State getState() {
    return state;
  }

  public void setState(State state) {
    State.checkTransition(getState(), state);
    if (this.state != NONE) { //Do not update timestamps during deserialization
      if (state == RUNNING)
        withStartedAt(System.currentTimeMillis()).withUpdatedAt(getStartedAt());
      else
        touch();

      if (state == SUCCEEDED)
        setEstimatedProgress(1);
    }
    this.state = state;
  }

  public T withState(State state) {
    setState(state);
    return (T) this;
  }

  /**
   * @return The estimated progress. A value from 0.0 to 1.0 (inclusive).
   */
  public float getEstimatedProgress() {
    return estimatedProgress;
  }

  public void setEstimatedProgress(float estimatedProgress) {
    touch();
    if(!Float.isNaN(estimatedProgress))
      this.estimatedProgress = estimatedProgress;
  }

  public T withEstimatedProgress(float estimatedProgress) {
    setEstimatedProgress(estimatedProgress);
    return (T) this;
  }

  public String getErrorMessage() {
    return errorMessage;
  }

  public void setErrorMessage(String errorMessage) {
    this.errorMessage = errorMessage;
  }

  public T withErrorMessage(String errorMessage) {
    setErrorMessage(errorMessage);
    return (T) this;
  }

  public String getErrorCause() {
    return errorCause;
  }

  public void setErrorCause(String errorCause) {
    this.errorCause = errorCause;
  }

  public T withErrorCause(String errorCause) {
    setErrorCause(errorCause);
    return (T) this;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(String errorCode) {
    this.errorCode = errorCode;
  }

  public T withErrorCode(String errorCode) {
    setErrorCode(errorCode);
    return (T) this;
  }

  public boolean isFailedRetryable() {
    return failedRetryable;
  }

  public void setFailedRetryable(boolean failedRetryable) {
    this.failedRetryable = failedRetryable;
  }

  public T withFailedRetryable(boolean failedRetryable) {
    setFailedRetryable(failedRetryable);
    return (T) this;
  }

  /**
   * Depicts the state of an executable task / flow from the perspective of the client which submitted it.
   */
  public enum State {
    NONE, //The initial state of all RuntimeInfo objects. This state is not a valid state and must be overwritten (e.g., by deserialization immediately after the creation of the RuntimeInfo object)
    NOT_READY, //The task is not ready to be submitted to the execution system yet. Not all pre-conditions are met.
    SUBMITTED, //The task is ready for execution, all necessary information was provided.
    PENDING, //The task is waiting to get executed by the target system. That could be dependent on execution resources to become available.
    RESUMING, //The task is preparing for a re-execution after a failure or cancellation.
    RUNNING, //The task is executing.
    CANCELLING, //The task is in the process of cancellation. Target systems are getting informed about that and resources are cleaned up.
    CANCELLED(true), //The task was successfully canceled. It can be resumed later.
    FAILED(true), //The task has failed. Depending on its inner state, it could be possible to resume it later.
    SUCCEEDED(true); //The task has been executed completely, all results are available. The task cannot be resumed.

    private final boolean isFinal;
    private static final Map<State, State[]> validSuccessors = ImmutableMap.of(
        NONE, new State[]{null}, //Allows all transitions at initialization. Needed to allow proper deserialization into the POJO.
        NOT_READY, new State[]{SUBMITTED, FAILED},
        SUBMITTED, new State[]{PENDING, CANCELLING, FAILED},
        PENDING, new State[]{RUNNING, CANCELLING, SUCCEEDED, FAILED},
        RESUMING, new State[]{PENDING, CANCELLING, FAILED},
        RUNNING, new State[]{SUCCEEDED, CANCELLING, FAILED},
        CANCELLING, new State[]{CANCELLED, FAILED},
        CANCELLED, new State[]{RESUMING},
        FAILED, new State[]{RESUMING},
        SUCCEEDED, new State[]{}
    );

    State() {
      this(false);
    }

    State(boolean isFinal) {
      this.isFinal = isFinal;
    }

    public boolean isFinal() {
      return isFinal;
    }

    public boolean isValidSuccessor(State successorState) {
      return Arrays.stream(validSuccessors.get(this))
          .anyMatch(validSuccessor -> validSuccessor == successorState || validSuccessor == null);
    }

    public static void checkTransition(State sourceState, State targetState) {
      if (sourceState != targetState && !sourceState.isValidSuccessor(targetState))
        throw new IllegalStateTransition(sourceState, targetState);
    }

    public static State of(String value) {
      if (value == null)
        return null;

      try {
        return valueOf(value.toUpperCase());
      }
      catch (IllegalArgumentException e) {
        return null;
      }
    }

    public static class IllegalStateTransition extends IllegalStateException {
      public final State sourceState;
      public final State targetState;

      public IllegalStateTransition(State sourceState, State targetState) {
        super("Illegal state transition: " + sourceState + " -> " + targetState);
        this.sourceState = sourceState;
        this.targetState = targetState;
      }
    }
  }
}
