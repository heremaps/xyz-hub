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
package com.here.naksha.lib.core.util.fib;

import com.here.naksha.lib.core.NakshaVersion;
import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Possible special expectations.
 */
@AvailableSince(NakshaVersion.v2_0_5)
public enum FibExpected {
  /**
   * Any value is okay, includes {@link #UNDEFINED} and {@code null}.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  ANY,

  /**
   * Expect the absence of the key.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  UNDEFINED,

  /**
   * Expect that the key exists, but the value does not matter.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  DEFINED,

  /**
   * Expect either the absence of a key or the value {@code null}.
   */
  @AvailableSince(NakshaVersion.v2_0_5)
  UNDEFINED_OR_NULL
}
