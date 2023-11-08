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

import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The result of a write-operation.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class WriteOpResult<T> {

  public WriteOpResult(@NotNull EExecutedOp op, @Nullable T feature) {
    this.op = op;
    this.feature = feature;
  }

  /**
   * The operation that has been performed.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull EExecutedOp op;

  /**
   * The feature that is the result of the operation; will be {@code null}, if the {@code noResult} argument was set.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable T feature;
}
