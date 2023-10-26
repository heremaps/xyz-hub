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

public class AdvancedWriteOp<T> extends WriteOp<T> {
  @AvailableSince(NakshaVersion.v2_0_7)
  public AdvancedWriteOp(
      @Nullable T feature,
      @Nullable Object patch,
      @Nullable String id,
      @Nullable String uuid,
      boolean noResult,
      @NotNull IfExists onExists,
      @NotNull IfConflict onConflict,
      @NotNull IfNotExists onNotExists) {
    super(EWriteOp.ADVANCED, feature, id, uuid, noResult);
    // TODO: Verify the parameter combinations!
    this.patch = patch;
    this.onExists = onExists;
    this.onConflict = onConflict;
    this.onNotExists = onNotExists;
  }

  /**
   * The patch to apply.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable Object patch;

  /**
   * The action to be performed if the object does exist already, with either not state requirement given ({@link WriteOp#uuid} being
   * {@code null}) or the state matches the required one.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull IfExists onExists;

  /**
   * The action to be performed if the object does exist already, but the state does not match the given (required) one
   * ({@link WriteOp#uuid}).
   */
  public final @NotNull IfConflict onConflict;

  /**
   * The action to be performed if the object does not exist. This action is executed even when a specific state was required
   * ({@link WriteOp#uuid}).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull IfNotExists onNotExists;
}
