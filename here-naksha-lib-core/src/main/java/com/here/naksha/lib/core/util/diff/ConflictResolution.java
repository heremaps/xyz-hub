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

import com.fasterxml.jackson.annotation.JsonCreator;
import org.jetbrains.annotations.Nullable;

/** The way conflicts are to be resolved by the modification entry. */
public enum ConflictResolution {
  /**
   * Whenever the current head state is different as the base state, abort the transaction and
   * report an error.
   */
  ERROR,

  /**
   * Whenever the current head state is different as the base state, keep the current head and
   * continue the transaction without error.
   */
  RETAIN,

  /**
   * Always replace the current head state with the given version, no matter if any other change
   * conflicts. In this case, the puuid will refer to the state being replaced.
   */
  REPLACE;

  /**
   * Whenever the current head state is different as the base state, try a <a
   * href="https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge>three-way-merge</a>
   * and if that fails, abort the transaction with an error.
   */
  // MERGE_OR_ERROR,

  /**
   * Whenever the current head state is different as the base state, try a <a
   * href="https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge>three-way-merge</a>
   * and conflicting properties or parts of the feature will be retained (stay unmodified).
   */
  // MERGE_OR_RETAIN,

  /**
   * Whenever the current head state is different as the base state, try a <a
   * href="https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge>three-way-merge</a>
   * and conflicting properties or parts of the feature will be replaced with the new version.
   */
  // MERGE_OR_REPLACE;

  /**
   * Returns the conflict resolution from the given text.
   *
   * @param value The text to parse.
   * @return The found conflict resolution; {@code null} if none matches.
   */
  @JsonCreator
  public static @Nullable ConflictResolution of(@Nullable String value) {
    if (value != null) {
      for (final ConflictResolution cr : ConflictResolution.values()) {
        if (cr.name().equalsIgnoreCase(value)) {
          return cr;
        }
      }
    }
    return null;
  }
}
