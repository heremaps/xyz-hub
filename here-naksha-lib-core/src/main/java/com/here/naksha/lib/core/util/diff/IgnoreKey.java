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
package com.here.naksha.lib.core.util.diff;

import java.util.Map;
import org.jetbrains.annotations.NotNull;

/** A method invoked to test if a key of a map should be ignored by the patcher. */
@FunctionalInterface
public interface IgnoreKey {

  /**
   * Tests whether the given key should be ignored by the patcher.
   *
   * @param key The key in question.
   * @param sourceMap The source map.
   * @param targetOrPatchMap The target map, or the partial patch map.
   * @return {@code true} if the key should be ignored; {@code false} otherwise.
   */
  @SuppressWarnings("rawtypes")
  boolean ignore(@NotNull Object key, @NotNull Map sourceMap, @NotNull Map targetOrPatchMap);
}
