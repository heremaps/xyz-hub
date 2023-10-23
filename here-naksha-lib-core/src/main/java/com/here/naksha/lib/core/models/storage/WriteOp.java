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
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A modification to be applied to an object, being either a feature or a collection.
 *
 * @param <T> The type of the object to modify.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class WriteOp<T> {

  @AvailableSince(NakshaVersion.v2_0_7)
  public WriteOp(
      @Nullable T object,
      @Nullable Object patch,
      @Nullable String id,
      @Nullable String uuid,
      boolean noResult,
      @NotNull IfExists onExists,
      @NotNull IfConflict onConflict,
      @NotNull IfNotExists onNotExists) {
    this.object = object;
    this.patch = patch;
    this.id = id;
    this.uuid = uuid;
    this.noResult = noResult;
    this.onExists = onExists;
    this.onConflict = onConflict;
    this.onNotExists = onNotExists;
  }

  /**
   * The object to modify, if a full state of the object is available.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable T object;

  /**
   * The patch to apply.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable Object patch;

  /**
   * The feature identifier.
   *
   * <b>Note</b>: This value is only used when {@link #object} is {@code null}, otherwise the {@code id} property of the object is used.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable String id;

  /**
   * The unique state identifier to expect when performing the action. If the object does not exist in exactly this state a conflict is
   * raised. If {@code null}, then the operation is not concurrency save.
   *
   * <b>Note</b>: This value is only used when {@link #object} is {@code null}, otherwise the {@code properties->@ns:com:here:xyz->uuid} is
   * used.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable String uuid;

  /**
   * If the modification should return the new object state in the result-set. Note that after the modification the {@link XyzNamespace}
   * will have been changed with new {@link XyzNamespace#getUuid() uuid} and other changes. It is recommended, and the default behavior, to
   * return the new state after the modification succeeded.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public final boolean noResult;

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
