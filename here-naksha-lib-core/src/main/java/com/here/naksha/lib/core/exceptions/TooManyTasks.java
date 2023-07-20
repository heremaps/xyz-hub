/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core.exceptions;

import com.here.naksha.lib.core.AbstractTask;

/**
 * Thrown when the maximum amount of tasks, defined in {@link AbstractTask#limit}, are reached, and yet another task should be executed. The
 * system ignores this limit for internal tasks.
 */
public class TooManyTasks extends RuntimeException {

  public TooManyTasks() {
    super("Maximum number of concurrent tasks (" + AbstractTask.limit.get() + ") reached");
  }
}
