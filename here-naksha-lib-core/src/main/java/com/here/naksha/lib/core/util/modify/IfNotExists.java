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
package com.here.naksha.lib.core.util.modify;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.storage.WriteOp;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.Nullable;

/** The action to perform if a feature does not exist. */
@JsonFormat(shape = Shape.STRING)
@AvailableSince(NakshaVersion.v2_0_7)
public enum IfNotExists {
  /**
   * The existing state should be retained, so the feature continues to not exist.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  RETAIN,

  /**
   * The transaction should be aborted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  ERROR,

  /**
   * The {@link WriteOp#feature} should be created.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  CREATE,

  /**
   * The feature should be purged (finally removed, so that there is no further trace in the HEAD).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  PURGE;

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonCreator
  public static @Nullable IfNotExists of(@Nullable String value) {
    if (value != null) {
      for (final IfNotExists e : IfNotExists.values()) {
        if (e.name().equalsIgnoreCase(value)) {
          return e;
        }
      }
    }
    return null;
  }
}
