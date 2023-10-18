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
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.storage.WriteOp;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.Nullable;

/**
 * If the feature exists, but the state (represented via {@link XyzNamespace#uuid}) differs from the requested one ({@link WriteOp#uuid}).
 */
@AvailableSince(NakshaVersion.v2_0_7)
@JsonFormat(shape = Shape.STRING)
public enum IfConflict {
  /**
   * The existing state should be retained.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  RETAIN,

  /**
   * The transaction should be aborted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  ERROR,

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
   * The existing state should be replaced with the given one in {@link WriteOp#feature}, overriding foreign changes.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  REPLACE,

  /**
   * The given {@link WriteOp#patch} should be applied, overriding foreign changes.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  PATCH,

  /**
   * The changes should be merged on-top of the foreign changes. If that fails, the result will be an error and the transaction is aborted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  MERGE_ERROR,

  /**
   * The changes should be merged on-top of the foreign changes, properties that conflict will be overridden with the value from
   * {@link WriteOp#feature}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  MERGE_OVERRIDE,

  /**
   * The changes should be merged on-top of the foreign changes, if a conflict is found, the merge should be aborted and the given
   * {@link WriteOp#patch} should be applied.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  MERGE_PATCH,

  /**
   * The changes should be merged on-top of the foreign changes, properties that conflict will be retained (the foreign changes win).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  MERGE_RETAIN;

  @AvailableSince(NakshaVersion.v2_0_7)
  @JsonCreator
  public static @Nullable IfConflict of(@Nullable String value) {
    if (value != null) {
      for (final IfConflict e : IfConflict.values()) {
        if (e.name().equalsIgnoreCase(value)) {
          return e;
        }
      }
    }
    return null;
  }
}
