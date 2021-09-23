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

package com.here.xyz.util;

import com.google.common.hash.Hashing;
import java.nio.charset.Charset;

public class Hasher {

  public static final String getHash(String toBeHashed) {
    //noinspection UnstableApiUsage
    return Hashing.murmur3_128()
        .newHasher()
        .putString(toBeHashed, Charset.defaultCharset())
        .hash()
        .toString();
  }

  public static final String getHash(byte[] bytes) {
    return Hashing.murmur3_128()
        .newHasher()
        .putBytes(bytes)
        .hash()
        .toString();
  }

}
