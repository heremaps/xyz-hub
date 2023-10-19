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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.Nullable;

/**
 * If the feature that should be modified does exist (optionally in the requested state, so having the provided {@link ModifyQuery#uuid}). If
 * the feature exists, but not in the requested state ({@link ModifyQuery#uuid} given, but does not match), then an {@link IfConflict} action is
 * execute instead of the {@link IfExists}.
 */
@AvailableSince(NakshaVersion.v2_0_7)
@JsonFormat(shape = Shape.STRING)
public enum IfExists {
  /**
   * The existing state should be retained.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  RETAIN,

  /**
   * The transaction should be aborted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  FAIL,

  /**
   * The existing state should be deleted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  DELETE,

  /**
   * The feature should be deleted and purged (finally removed, so that there is no further trace in the HEAD).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  PURGE,

  /**
   * The existing state should be replaced with the given one in {@link ModifyQuery#object}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  REPLACE,

  /**
   * The given {@link ModifyQuery#patch} should be applied.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  PATCH,

  @Deprecated
  MERGE;

  @JsonCreator
  public static @Nullable IfExists of(@Nullable String value) {
    if (value != null) {
      for (final IfExists e : IfExists.values()) {
        if (e.name().equalsIgnoreCase(value)) {
          return e;
        }
      }
    }
    return null;
  }
}
