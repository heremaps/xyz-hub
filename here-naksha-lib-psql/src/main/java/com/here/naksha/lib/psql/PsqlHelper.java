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
package com.here.naksha.lib.psql;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.jetbrains.annotations.NotNull;

/**
 * A collection of helper functions.
 */
public class PsqlHelper {

  private static @NotNull MessageDigest newMd5() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new Error(e);
    }
  }

  /**
   * A thread local MD5 digest.
   */
  public static final @NotNull ThreadLocal<@NotNull MessageDigest> md5 = ThreadLocal.withInitial(PsqlHelper::newMd5);

  private static final String[] partExtension = new String[256];

  static {
    for (int i = 0; i < 256; i++) {
      partExtension[i] = (i < 10 ? "00" + i : i < 100 ? "0" + i : "" + i);
    }
  }

  /**
   * Returns the partition ID
   *
   * @param id The feature-id.
   * @return The partition identifier.
   */
  public static int partitionId(@NotNull String id) {
    final MessageDigest md5 = PsqlHelper.md5.get();
    md5.reset();
    final byte[] digest = md5.digest(id.getBytes(StandardCharsets.UTF_8));
    return ((int) digest[0]) & 63;
  }

  /**
   * Returns the partition extension for the given partition identifier.
   *
   * @param part_id The partition identifier (0-255).
   * @return The partition name.
   */
  public static @NotNull String partitionExtension(int part_id) {
    return partExtension[part_id];
  }
}
