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
import com.here.naksha.lib.core.util.json.JsonEnum;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/** The action to perform if a feature does not exist. */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_7)
public class IfNotExists extends JsonEnum {

  /**
   * The existing state should be retained, so the feature continues to not exist.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfNotExists RETAIN = def(IfNotExists.class, "RETAIN");

  /**
   * The transaction should be aborted.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfNotExists FAIL =
      def(IfNotExists.class, "FAIL").alias(IfNotExists.class, "ERROR").alias(IfNotExists.class, "ABORT");

  /**
   * The {@link AdvancedWriteOp#feature} should be created.
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfNotExists CREATE = def(IfNotExists.class, "CREATE");

  /**
   * The feature should be purged (finally removed, so that there is no further trace in the HEAD).
   */
  @AvailableSince(NakshaVersion.v2_0_7)
  public static final IfNotExists PURGE = def(IfNotExists.class, "PURGE");

  @Override
  protected void init() {
    register(IfNotExists.class);
  }
}
