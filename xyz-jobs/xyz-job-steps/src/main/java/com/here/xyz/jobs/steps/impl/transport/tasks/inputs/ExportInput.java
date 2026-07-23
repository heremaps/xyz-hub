/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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
package com.here.xyz.jobs.steps.impl.transport.tasks.inputs;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;
import static com.here.xyz.FeatureChange.Operation;

@JsonTypeName("ExportInput")
public record ExportInput(Integer threadId, String tileId, Long startI, Long endI, Operation operation) implements TaskPayload {
  public ExportInput(String tileId) {
    this(null, tileId, null, null, null);
  }

  /**
   * Persists the pre-calculated i-range boundaries of this task together with the task input. Storing them here ensures
   * they are kept inside the {@code task_input} column of the job data table and therefore stay stable across resumes -
   * even if writes happened in between - instead of being recomputed against a changed table state.
   */
  public ExportInput(Integer threadId, long startI, long endI) {
    this(threadId, null, startI, endI, null);
  }

  public ExportInput(Integer threadId, long startI, long endI,  Operation operation) {
    this(threadId, null, startI, endI, operation);
  }
}
