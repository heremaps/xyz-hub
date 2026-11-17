/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

package com.here.xyz.iceberg;

import static com.here.xyz.responses.XyzError.PAYLOAD_TO_LARGE;
import static com.here.xyz.responses.XyzError.TIMEOUT;

import java.util.concurrent.atomic.AtomicLong;

/**
 * The limits of one request: the bytes read from storage and the deadline.
 */
final class ReadBudget {

  private final AtomicLong bytesRead = new AtomicLong();
  private final long maxBytes;
  private final long deadlineMillis;

  ReadBudget(long maxBytes, long deadlineMillis) {
    this.maxBytes = maxBytes;
    this.deadlineMillis = deadlineMillis;
  }

  /**
   * Counts bytes read from storage.
   *
   * @throws ScanException with PAYLOAD_TO_LARGE once more than the allowed bytes were read
   */
  void addBytesRead(long bytes) {
    if (bytesRead.addAndGet(bytes) > maxBytes)
      throw new ScanException(PAYLOAD_TO_LARGE, "The request would read more than " + maxBytes + " bytes from Iceberg");
  }

  long bytesRead() {
    return bytesRead.get();
  }

  /**
   * @throws ScanException with TIMEOUT once the deadline has passed
   */
  void checkDeadline() {
    if (System.currentTimeMillis() >= deadlineMillis)
      throw new ScanException(TIMEOUT, "The Iceberg read did not finish in time");
  }
}
