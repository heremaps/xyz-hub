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

package com.here.xyz.httpconnector.job;

import static com.here.xyz.httpconnector.job.RuntimeInfo.State.NOT_READY;
import static com.here.xyz.httpconnector.job.RuntimeInfo.State.SUBMITTED;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.collect.ImmutableMap;
import com.here.xyz.hub.Core;
import java.util.Arrays;
import java.util.Map;

public class RuntimeInfo<T extends RuntimeInfo> {

  private long updatedAt;
  private long startedAt;
  private State state = NOT_READY;

  @JsonIgnore //TODO: Re-activate once implemented
  public float getEstimatedProgress() {
    return 0.5f; //TODO: Deduct from statistics, start time & e.g. estimated row count
  }

  @JsonIgnore //TODO: Re-activate once implemented
  public long getEstimatedEndTime() {
    long executionTime = Core.currentTimeMillis() - getStartedAt();
    float estimatedProgress = getEstimatedProgress();
    if (estimatedProgress == 0)
      return -1;
    long estimatedDuration = (long) (executionTime / estimatedProgress);
    return getStartedAt() + estimatedDuration;
  }

  /**
   * Returns the time of the last status update of the job in milliseconds.
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
    State.checkTransition(getState(), SUBMITTED);
    this.state = state;
  }

  public T withState(State state) {
    setState(state);
    return (T) this;
  }

  /**
   * Depicts the state of an executable task / flow from the perspective of the client which submitted it.
   */
  public enum State {
    NOT_READY, //The task is not ready to be submitted to the execution system yet. Not all pre-conditions are met.
    SUBMITTED, //The task is ready for execution, all needed information was provided.
    PENDING, //The task is waiting to get executed by the target system. That could be dependent by execution resources to become available.
    RESUMING, //The task is preparing for a re-execution after a failure or cancellation.
    RUNNING, //The task is executing.
    CANCELLING, //The task is in the process of cancellation. Target systems are getting informed about that and resources are cleaned up.
    CANCELLED(true), //The task was successfully canceled. It can be resumed later.
    FAILED(true), //The task has failed. Depending on its inner state, it could be possible to resume it later.
    SUCCEEDED(true); //The task has been executed completely, all results are available. The task cannot be resumed.

    private final boolean isFinal;
    private static final Map<State, State[]> validSuccessors = ImmutableMap.of(
        NOT_READY, new State[]{SUBMITTED, FAILED},
        SUBMITTED, new State[]{PENDING, CANCELLING, FAILED},
        PENDING, new State[]{RUNNING, CANCELLING, FAILED},
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
          .anyMatch(validSuccessor -> validSuccessor == successorState);
    }

    public static void checkTransition(State sourceState, State targetState) {
      if (!sourceState.isValidSuccessor(targetState))
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
