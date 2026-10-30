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

package com.here.xyz.jobs.steps.impl.transport;

import static com.here.xyz.jobs.RuntimeInfo.State.RUNNING;
import static com.here.xyz.jobs.steps.impl.transport.TaskedSpaceBasedStep.NO_TASKS_SUCCESS_CALLBACK_TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.xyz.XyzSerializable;
import com.here.xyz.XyzSerializable.Static;
import com.here.xyz.jobs.steps.Config;
import java.lang.reflect.Field;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A step without task items reports its success through the success callback which it requests during its execution.
 * A STATE_CHECK must only report the success as a fallback, if that callback did not arrive in time.
 */
class TaskedSpaceBasedStepExecutionStateTest {

  @BeforeAll
  static void initConfig() {
    if (Config.instance == null)
      new Config();
  }

  @Test
  void noTasksCreated_waitsForSuccessCallback() throws Exception {
    TaskedImportFilesToSpace step = stepWithoutTasks(System.currentTimeMillis() - 1000);
    assertEquals("RUNNING", String.valueOf(step.getExecutionState()));
  }

  @Test
  void noTasksCreated_succeedsWithoutSuccessCallback() throws Exception {
    TaskedImportFilesToSpace step = stepWithoutTasks(System.currentTimeMillis() - NO_TASKS_SUCCESS_CALLBACK_TIMEOUT - 1000);
    assertEquals("SUCCEEDED", String.valueOf(step.getExecutionState()));
  }

  @Test
  void noTasksCreated_heartbeatDoesNotPostponeFallback() throws Exception {
    TaskedImportFilesToSpace step = stepWithoutTasks(System.currentTimeMillis() - NO_TASKS_SUCCESS_CALLBACK_TIMEOUT - 1000);
    //The status update of a RUNNING heartbeat (RUNNING -> RUNNING) resets status.startedAt
    long beforeHeartbeat = System.currentTimeMillis();
    step.getStatus().setState(RUNNING);
    assertTrue(step.getStatus().getStartedAt() >= beforeHeartbeat);

    assertEquals("SUCCEEDED", String.valueOf(step.getExecutionState()));
  }

  @Test
  void noTasksCreated_noSuccessCallbackRequested() throws Exception {
    //E.g., a step that was started by a previous version, which did not request a success callback
    TaskedImportFilesToSpace step = stepWithoutTasks(0);
    assertEquals("SUCCEEDED", String.valueOf(step.getExecutionState()));
  }

  @Test
  void noTasksCreated_requestTimeIsPersisted() throws Exception {
    TaskedImportFilesToSpace step = stepWithoutTasks(System.currentTimeMillis() - 1000);
    TaskedImportFilesToSpace storedStep = XyzSerializable.deserialize(step.serialize(Static.class), TaskedImportFilesToSpace.class);
    assertEquals("RUNNING", String.valueOf(storedStep.getExecutionState()));
  }

  private static TaskedImportFilesToSpace stepWithoutTasks(long successCallbackRequestedAt) throws ReflectiveOperationException {
    TaskedImportFilesToSpace step = new TaskedImportFilesToSpace().withSpaceId("someSpace");
    step.getStatus().withState(RUNNING);
    setField(step, "noTasksCreated", true);
    setField(step, "successCallbackRequestedAt", successCallbackRequestedAt);
    return step;
  }

  private static void setField(TaskedSpaceBasedStep step, String name, Object value) throws ReflectiveOperationException {
    Field field = TaskedSpaceBasedStep.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(step, value);
  }
}
