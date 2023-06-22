/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

public class Hex {

  /** An array from 0 to 15 that holds the corresponding hex character representations. */
  public static final char[] valueToChar =
      new char[] {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

  private static final int[] charToValue = {
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, // 0x00 - 0x0F
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, // 0x10 - 0x1F
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, // 0x20 - 0x2F
    +0,
    +1,
    +2,
    +3,
    +4,
    +5,
    +6,
    +7,
    +8,
    +9,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, // 0x30 - 0x3F '0'-'9'
    -1,
    10,
    11,
    12,
    13,
    14,
    15,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, // 0x40 - 0x4F 'A'-'F'
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1, // 0x50 - 0x5F
    -1,
    10,
    11,
    12,
    13,
    14,
    15,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1 // 0x60 - 0x6F 'a'-'f'
  };

  /**
   * Returns the value of the given hex-character or minus one, if the provided character is no
   * valid hex character.
   *
   * @param c the hex character to decode.
   * @return the value from 0 to 15 or -1, if the character is invalid.
   */
  public static int valueOf(char c) {
    if (c >= charToValue.length) {
      return -1;
    }

    return charToValue[c];
  }

  /**
   * Returns the value of the given hex-character or minus one, if the provided character is no
   * valid hex character.
   *
   * @param c the hex character to decode.
   * @param alt the alternative to return, when the character is invalid.
   * @return the value from 0 to 15 or the alternative, if the character is invalid.
   */
  public static int valueOf(char c, int alt) {
    int value = valueOf(c);
    return (value == -1) ? alt : value;
  }

  public static int decode(char high, char low) throws IllegalArgumentException {
    final int highValue = valueOf(high);
    if (highValue < 0) {
      throw new IllegalArgumentException();
    }
    final int lowValue = valueOf(low);
    if (lowValue < 0) {
      throw new IllegalArgumentException();
    }
    return (highValue << 4) | (lowValue);
  }
}
