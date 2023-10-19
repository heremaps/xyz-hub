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
package com.here.naksha.lib.core.models.storage;

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * The result of a modification.
 *
 * @param <T> the type of the object returned.
 */
public class WriteResult<T> extends SuccessResult {

  /**
   * Create an empty result-set.
   */
  public WriteResult() {
    this(new ArrayList<>());
  }

  /**
   * Create a result-set from the given modification result list.
   * @param results the list to use.
   */
  public WriteResult(@NotNull List<WriteOpResult<T>> results) {
    this.results = results;
  }

  /**
   * The results.
   */
  public @NotNull List<WriteOpResult<T>> results;
}
