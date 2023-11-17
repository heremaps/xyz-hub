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
 * A basic class for everything that somehow boxes a feature. This can be extended to enrich the basic functionality.
 *
 * @param <FEATURE> The feature-type.
 * @param <SELF>    The implementation type.
 */
@AvailableSince(NakshaVersion.v2_0_7)
public class FeatureBox<FEATURE, SELF extends FeatureBox<FEATURE, SELF>> {

  /**
   * The stored feature.
   */
  protected @Nullable FEATURE feature;

  /**
   * The operation linked to the feature.
   */
  protected @NotNull EStorageOp op = EStorageOp.NULL;

  /**
   * Returns this.
   *
   * @return this.
   */
  @SuppressWarnings("unchecked")
  protected final @NotNull SELF self() {
    return (SELF) this;
  }

  /**
   * Clears the box and all values.
   *
   * @return this.
   */
  public @NotNull SELF clear() {
    this.feature = null;
    this.op = EStorageOp.NULL;
    return self();
  }

  /**
   * Returns or produces the boxed feature; if any.
   *
   * @return or produces the boxed feature; if any.
   */
  public @Nullable FEATURE getFeature() {
    return feature;
  }

  /**
   * Sets the feature and returns the previously boxed feature; if any.
   *
   * @param feature The feature to set.
   * @return The previously boxed feature; if any.
   */
  public @Nullable Object setFeature(@Nullable FEATURE feature) {
    final Object old = this.feature;
    this.feature = feature;
    return old;
  }

  /**
   * Sets the feature, for streaming usage.
   *
   * @param feature The feature to set.
   * @return this.
   */
  public final @NotNull SELF withFeature(@Nullable FEATURE feature) {
    setFeature(feature);
    return self();
  }

  /**
   * Returns the operation linked to the boxed feature.
   *
   * @return the operation linked to the boxed feature.
   */
  public @NotNull EStorageOp getOp() {
    return op;
  }

  /**
   * Sets the operation linked to the boxed feature.
   *
   * @param op The operation to set.
   * @return The previously set operation.
   */
  public @NotNull EStorageOp setOp(@NotNull EStorageOp op) {
    final EStorageOp old = this.op;
    this.op = op;
    return old;
  }

  /**
   * Sets the operation linked to the boxed feature.
   *
   * @param op The operation to set.
   * @return this.
   */
  public final @NotNull SELF withFeature(@NotNull EStorageOp op) {
    setOp(op);
    return self();
  }
}
