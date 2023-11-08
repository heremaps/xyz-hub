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
import com.here.naksha.lib.core.util.json.JsonEnum;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * If the feature that should be modified does exist (optionally in the requested state, so having the provided {@link WriteOp#uuid}). If
 * the feature exists, but not in the requested state ({@link WriteOp#uuid} given, but does not match), then an {@link IfConflict} action is
 * execute instead of the {@link IfExists}.
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_7)
public class IfExists extends JsonEnum {

  /**
   * The existing state should be retained.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfExists RETAIN = def(IfExists.class, "RETAIN");

  /**
   * The transaction should be aborted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfExists FAIL =
      def(IfExists.class, "FAIL").alias(IfExists.class, "ERROR").alias(IfExists.class, "ABORT");

  /**
   * The existing state should be deleted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfExists DELETE = def(IfExists.class, "DELETE");

  /**
   * The feature should be deleted and purged (finally removed, so that there is no further trace in the HEAD).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfExists PURGE = def(IfExists.class, "PURGE");

  /**
   * The existing state should be replaced with the given one in {@link WriteOp#feature}.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfExists REPLACE = def(IfExists.class, "REPLACE");

  /**
   * The given {@link AdvancedWriteOp#patch} should be applied.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfExists PATCH = def(IfExists.class, "PATCH");

  @Deprecated
  public static final IfExists MERGE = def(IfExists.class, "MERGE");

  @Override
  protected void init() {
    register(IfExists.class);
  }
}
