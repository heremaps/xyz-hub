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
package com.here.naksha.lib.core.util.fib;

import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Operations that can be done to a {@link FibSet}.
 */
@AvailableSince(NakshaVersion.v2_0_5)
public enum FibSetOp {
  /**
   * Return existing entry, but do not create, when no entry for the key exists.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  GET(true),

  /**
   * Return the existing entry or create a new one, when no entry exists for the key, upgrades references.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  PUT(false),

  /**
   * Remove existing entry and return it; if any exists.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  REMOVE(false);

  FibSetOp(boolean readOnly) {
    this.readOnly = readOnly;
  }

  /**
   * Indicating if this operation is read-only.
   */
  public final boolean readOnly;
}
