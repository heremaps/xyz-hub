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
package com.here.naksha.lib.psql;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.jetbrains.annotations.NotNull;

class PostgresWriteFeaturesToPartition<T> {

  private static @NotNull MessageDigest newMd5() {
    try {
      return MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new Error(e);
    }
  }

  private static final ThreadLocal<MessageDigest> md5ThreadLocal =
      ThreadLocal.withInitial(PostgresWriteFeaturesToPartition::newMd5);

  static int partitionIdOf(@NotNull String id) {
    final MessageDigest md5 = md5ThreadLocal.get();
    md5.reset();
    final byte[] digest = md5.digest(id.getBytes(StandardCharsets.UTF_8));
    return ((int) digest[0]) & 63;
  }

  PostgresWriteFeaturesToPartition(@NotNull String collectionId, int partitionId) {
    this.partitionId = partitionId;
  }

  final int partitionId;
}
