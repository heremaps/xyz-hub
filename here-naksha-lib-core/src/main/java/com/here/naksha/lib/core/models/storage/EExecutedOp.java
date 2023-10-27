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
 * The operation that was actually executed.
 */
@JsonFormat(shape = Shape.STRING)
@AvailableSince(NakshaVersion.v2_0_7)
public enum EExecutedOp {
  /**
   * A new feature was created.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  INSERTED,

  /**
   * The existing state was retained.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  RETAINED,

  /**
   * The existing state was deleted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  DELETED,

  /**
   * The feature was deleted and purged (finally removed, so that there is no further trace in the HEAD).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  PURGED,

  /**
   * The changes were updated with foreign changes.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  UPDATED;

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonCreator
  public static @Nullable EExecutedOp of(@Nullable String value) {
    if (value != null) {
      for (final EExecutedOp e : EExecutedOp.values()) {
        if (e.name().equalsIgnoreCase(value)) {
          return e;
        }
      }
    }
    return null;
  }
}
