/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
import com.here.naksha.lib.core.util.json.JsonEnum;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * If the feature exists, but the state (represented via {@link XyzNamespace#uuid}) differs from the requested one ({@link WriteOp#uuid}).
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_7)
public class IfConflict extends JsonEnum {
  /**
   * The existing state should be retained.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfConflict RETAIN = def(IfConflict.class, "RETAIN");

  /**
   * The transaction should be aborted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfConflict FAIL =
      def(IfConflict.class, "FAIL").alias(IfConflict.class, "ERROR").alias(IfConflict.class, "ABORT");

  /**
   * The existing state should be deleted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfConflict DELETE = def(IfConflict.class, "DELETE");

  /**
   * The feature should be deleted and purged (finally removed, so that there is no further trace in the HEAD).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfConflict PURGE = def(IfConflict.class, "PURGE");

  /**
   * The existing state should be replaced with the given one in {@link WriteOp#feature}, overriding foreign changes.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfConflict REPLACE = def(IfConflict.class, "REPLACE");

  /**
   * The given {@link AdvancedWriteOp#patch} should be applied, overriding foreign changes.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfConflict PATCH = def(IfConflict.class, "PATCH");

  /**
   * The changes should be merged on-top of the foreign changes. If that fails, the result will be an error and the transaction is aborted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfConflict MERGE_OR_FAIL = def(IfConflict.class, "MERGE_OR_FAIL");

  /**
   * The changes should be merged on-top of the foreign changes, properties that conflict will be overridden with the value from
   * {@link WriteOp#feature} (foreign changes loose).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfConflict MERGE_OR_OVERRIDE = def(IfConflict.class, "MERGE_OR_OVERRIDE");

  /**
   * The changes should be merged on-top of the foreign changes, properties that conflict will be retained (the foreign changes win).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfConflict MERGE_OR_RETAIN = def(IfConflict.class, "MERGE_OR_RETAIN");

  @Override
  protected void init() {
    register(IfConflict.class);
  }
}
