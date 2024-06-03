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
package com.here.naksha.lib.core.util;

import org.jetbrains.annotations.ApiStatus.AvailableSince;

/**
 * Helper class to combine two 32-bit values into one 64-bit value.
 *
 * @since 2.0.0
 */
@AvailableSince("2.0.0")
public final class LH64 {

  /**
   * Combine the low and high 32-bit integers to one 64-bit integer.
   *
   * @param low The lower 32-bit value.
   * @param high The higher 32-bit value.
   * @return The 64-bit value.
   */
  @AvailableSince("2.0.0")
  public static long lh64(int low, int high) {
    return ((high & 0xffffffffL) << 32) | (low & 0xffffffffL);
  }

  /**
   * Extract the higher 32-bit value.
   *
   * @param lh The combined 64-bit value.
   * @return The higher 32-bit value.
   */
  @AvailableSince("2.0.0")
  public static int highInt(long lh) {
    return (int) (lh >>> 32);
  }

  /**
   * Extract the lower 32-bit value.
   *
   * @param lh The combined 64-bit value.
   * @return The lower 32-bit value.
   */
  @AvailableSince("2.0.0")
  public static int lowInt(long lh) {
    return (int) (lh & 0xffffffffL);
  }
}
