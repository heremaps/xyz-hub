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

import static com.here.xyz.jobs.steps.impl.transport.TaskedSpaceBasedStep.limitByDatabaseTaskSlots;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/**
 * A tasked step with a database limit (TaskedSpaceBasedStep#getMaxTasksPerDatabase()) only starts tasks within the free task slots
 * of its database. The task queries of all tasked steps on the database are counted.
 */
class TaskedSpaceBasedStepDatabaseTaskLimitTest {

  @Test
  void noLimit_keepsTheStartableTasks() {
    assertEquals(6, limitByDatabaseTaskSlots(6, 3, 0, 500));
    assertEquals(6, limitByDatabaseTaskSlots(6, 3, -1, null));
  }

  @Test
  void limit_startsOnlyTheFreeSlots() {
    assertEquals(2, limitByDatabaseTaskSlots(6, 3, 60, 58));
  }

  @Test
  void limit_neverStartsMoreThanTheStepMay() {
    assertEquals(3, limitByDatabaseTaskSlots(3, 0, 60, 10));
  }

  @Test
  void limitReached_stepWithRunningTasksStartsNone() {
    assertEquals(0, limitByDatabaseTaskSlots(6, 3, 60, 60));
    assertEquals(0, limitByDatabaseTaskSlots(6, 3, 60, 75));
  }

  @Test
  void limitReached_stepWithoutRunningTasksStartsOne() {
    //Otherwise the state check of the step would find no running task and the step would fail after the heartbeat timeout
    assertEquals(1, limitByDatabaseTaskSlots(3, 0, 60, 75));
  }

  @Test
  void unknownRunningTasks_areTreatedAsLimitReached() {
    assertEquals(0, limitByDatabaseTaskSlots(6, 3, 60, null));
    assertEquals(1, limitByDatabaseTaskSlots(3, 0, 60, null));
  }

  @Test
  void nothingStartable_startsNone() {
    assertEquals(0, limitByDatabaseTaskSlots(0, 0, 60, 0));
    assertEquals(0, limitByDatabaseTaskSlots(-2, 0, 0, null));
  }
}
