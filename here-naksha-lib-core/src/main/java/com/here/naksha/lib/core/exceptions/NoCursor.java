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

import com.here.naksha.lib.core.models.storage.Result;
import org.jetbrains.annotations.NotNull;

/**
 * Throw when a {@link Result} is tried to be cast to a {@link ResultCursor} via {@link Result#cursor(Class)}.
 */
public class NoCursor extends Exception {

  public NoCursor(@NotNull Result result) {
    this.result = result;
  }

  /**
   * The result that failed to be cast to a {@link ResultCursor}.
   */
  public @NotNull Result result;
}
