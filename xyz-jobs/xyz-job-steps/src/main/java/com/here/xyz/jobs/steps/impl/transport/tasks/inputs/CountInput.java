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

import com.here.xyz.jobs.steps.impl.transport.tasks.TaskPayload;

/**
 * Input payload for a single partition of a parallelized space count.
 *
 * @param s            A descriptive label for the task.
 * @param threadId     The zero-based index of this partition.
 * @param threadCount  The total number of partitions the count is split into.
 */
public record CountInput(String s, int threadId, int threadCount) implements TaskPayload {
  public CountInput(String s) {
    this(s, 0, 1);
  }
}
