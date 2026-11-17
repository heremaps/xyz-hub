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

import static com.here.xyz.jobs.steps.execution.LambdaBasedStep.INVALID_TOKEN_RETRY_DELAYS_MS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.services.sfn.model.InvalidTokenException;
import software.amazon.awssdk.services.sfn.model.TaskTimedOutException;

class LambdaBasedStepHeartbeatTest {
  private static final String STEP_ID = "job.s_step";
  private final List<Long> sleeps = new ArrayList<>();
  private int calls;

  @AfterEach
  void clearInterrupt() {
    Thread.interrupted();
  }

  @Test
  void acceptedHeartbeatIsSentOnce() {
    assertTrue(LambdaBasedStep.sendHeartbeat(() -> calls++, sleeps::add, STEP_ID));
    assertEquals(1, calls);
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void invalidTokenIsRetriedUntilTheHeartbeatIsAccepted() {
    //The task is not known yet for the first two heartbeats, e.g., while the START_EXECUTION invocation is still running
    Runnable heartbeat = () -> {
      if (++calls <= 2)
        throw InvalidTokenException.builder().message("Invalid token").build();
    };

    assertTrue(LambdaBasedStep.sendHeartbeat(heartbeat, sleeps::add, STEP_ID));
    assertEquals(3, calls);
    assertEquals(List.of(INVALID_TOKEN_RETRY_DELAYS_MS[0], INVALID_TOKEN_RETRY_DELAYS_MS[1]), sleeps);
  }

  @Test
  void invalidTokenFailsAfterAllRetries() {
    Runnable heartbeat = () -> {
      calls++;
      throw InvalidTokenException.builder().message("Invalid token").build();
    };

    assertFalse(LambdaBasedStep.sendHeartbeat(heartbeat, sleeps::add, STEP_ID));
    assertEquals(INVALID_TOKEN_RETRY_DELAYS_MS.length + 1, calls);
    assertEquals(Arrays.stream(INVALID_TOKEN_RETRY_DELAYS_MS).boxed().toList(), sleeps);
  }

  @Test
  void timedOutTaskFailsWithoutRetry() {
    Runnable heartbeat = () -> {
      calls++;
      throw TaskTimedOutException.builder().message("Provided task does not exist anymore").build();
    };

    assertFalse(LambdaBasedStep.sendHeartbeat(heartbeat, sleeps::add, STEP_ID));
    assertEquals(1, calls);
    assertTrue(sleeps.isEmpty());
  }

  @Test
  void interruptedRetryFails() {
    Runnable heartbeat = () -> {
      calls++;
      throw InvalidTokenException.builder().message("Invalid token").build();
    };

    assertFalse(LambdaBasedStep.sendHeartbeat(heartbeat, millis -> {
      throw new InterruptedException();
    }, STEP_ID));
    assertEquals(1, calls);
    assertTrue(Thread.currentThread().isInterrupted());
  }
}
