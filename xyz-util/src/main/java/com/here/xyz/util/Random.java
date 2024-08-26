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

package com.here.xyz.util;

public class Random {
  public static String randomAlpha() {
    return randomAlpha(10);
  }

  public static String randomAlpha(int length) {
    return new java.util.Random()
        .ints(length, 'a', 'z')
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  public static String randomAlphaNumeric(int length) {
    char numericStart = '0';
    char numericEnd = '9';
    char upperCaseStart = 'A';
    char upperCaseEnd = 'Z';
    char lowerCaseStart = 'a';
    char lowerCaseEnd = 'z';

    return new java.util.Random()
        .ints(numericStart, lowerCaseEnd + 1)
        .filter(i -> (i <= numericEnd || i >= upperCaseStart) && (i <= upperCaseEnd || i >= lowerCaseStart))
        .limit(length)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }
}
