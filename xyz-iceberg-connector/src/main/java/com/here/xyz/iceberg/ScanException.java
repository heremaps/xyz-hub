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

import com.here.xyz.responses.XyzError;

/**
 * Stops a scan with a specific error, e.g. when a limit of the request was hit.
 * It is unchecked, so it can be thrown from inside Iceberg's and Parquet's readers.
 */
class ScanException extends RuntimeException {

  private final XyzError error;

  ScanException(XyzError error, String message) {
    super(message);
    this.error = error;
  }

  XyzError error() {
    return error;
  }
}
