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

package com.here.xyz.util.db.pg;

import java.util.Arrays;

public enum SQLError {
  ILLEGAL_ARGUMENT("XYZ40"),
  FEATURE_EXISTS("XYZ20"),
  FEATURE_NOT_EXISTS("XYZ44"),
  MERGE_CONFLICT_ERROR("XYZ48"),
  VERSION_CONFLICT_ERROR("XYZ49"),
  XYZ_EXCEPTION("XYZ50"),
  UNKNOWN("");

  public final String errorCode;

  SQLError(String errorCode) {
    this.errorCode = errorCode;
  }

  public String getErrorCode() {
    return errorCode;
  }

  public static SQLError fromErrorCode(String errorCode) {
    return Arrays.stream(values()).filter(error -> error.errorCode.equals(errorCode)).findFirst().orElse(UNKNOWN);
  }
}
