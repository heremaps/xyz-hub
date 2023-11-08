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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.vividsolutions.jts.geom.Geometry;
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

  /**
   * Create a write operation.
   *
   * @param op        The operation to perform.
   * @param id        The identifier of the feature.
   * @param uuid      The optional expected state identifier to force atomic operations. If given, the operation will fail, when the feature
   *                  is not in this state.
   * @param feature   The feature the operation applies to.
   * @param geometry  The geometry the operation applies to.
   * @param minResult {@code true} if a minimal result should be returned, that means the feature and geometry is not returned.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public WriteOp(
      @NotNull EWriteOp op,
      @NotNull String id,
      @Nullable String uuid,
      @Nullable T feature,
      @Nullable Geometry geometry,
      boolean minResult) {
    this.op = op;
    this.id = id;
    this.uuid = uuid;
    this.feature = feature;
    this.geometry = geometry;
    this.minResult = minResult;
  }

  /**
   * The operation to perform.
   */
  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull EWriteOp op;

  /**
   * The object to modify, if a full state of the object is available.
   */
  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable T feature;

  /**
   * The geometry.
   */
  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable Geometry geometry;

  /**
   * The feature identifier.
   */
  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @NotNull String id;

  /**
   * The unique state identifier to expect when performing the action. If the object does not exist in exactly this state a conflict is
   * raised. If {@code null}, then the operation is not concurrency save.
   */
  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  public final @Nullable String uuid;

  /**
   * If the modification should return the new object state in the result-set. Note that after the modification the {@link XyzNamespace}
   * will have been changed with new {@link XyzNamespace#getUuid() uuid} and other changes. It is recommended, and the default behavior, to
   * return the new state after the modification succeeded.
   */
  @JsonProperty
  @AvailableSince(NakshaVersion.v2_0_7)
  public final boolean minResult;
}
