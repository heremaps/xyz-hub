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
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.util.modify.IfConflict;
import com.here.naksha.lib.core.util.modify.IfExists;
import com.here.naksha.lib.core.util.modify.IfNotExists;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A write operation wrapper.
 *
 * @param <T> The type of the feature to write.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class WriteOp<T> {

  @AvailableSince(NakshaVersion.v2_0_7)
  public WriteOp(
      @Nullable T feature,
      @Nullable Object patch,
      @Nullable String id,
      @Nullable String uuid,
      boolean doNotReturnFeature,
      @NotNull IfExists onExists,
      @NotNull IfConflict onConflict,
      @NotNull IfNotExists onNotExists) {
    this.feature = feature;
    this.patch = patch;
    this.id = id;
    this.uuid = uuid;
    this.doNotReturnFeature = doNotReturnFeature;
    this.onExists = onExists;
    this.onConflict = onConflict;
    this.onNotExists = onNotExists;
  }

  /**
   * The feature to write, if a full feature is available.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable T feature;

  /**
   * The patch to apply; if any.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable Object patch;

  /**
   * The feature identifier; if any.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable String id;

  /**
   * The unique state identifier to expect when performing the action. If the feature does not exist in exactly this state a conflict is
   * raised. If {@code null}, then the operation is not concurrency save.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable String uuid;

  /**
   * If the write operation should return the new feature state in the result-set. Note that after writing the {@link XyzNamespace} will
   * have been changed with new {@link XyzNamespace#getUuid() uuid} and others. It is recommended and the default behavior to return the new
   * state after the operation succeeded.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final boolean doNotReturnFeature;

  /**
   * The action to be performed if the features does exist already, with either not state requirement given ({@link WriteOp#uuid} being
   * {@code null}) or the state matches the required one.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull IfExists onExists;

  /**
   * The action to be performed if the features does exist already, but the state does not match the given (required) one
   * ({@link WriteOp#uuid}).
   */
  public final @NotNull IfConflict onConflict;

  /**
   * The action to be performed if the feature does not exist. This action is executed even when a specific state was required
   * ({@link WriteOp#uuid}).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull IfNotExists onNotExists;
}
