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

import com.here.naksha.lib.core.models.XyzError;
import java.util.HashMap;
import java.util.Map;

public class XyzErrorMapper {

  private static final Map<String, XyzError> PSQL_ERROR_MAP = new HashMap<>();

  static {
    // UNINITIALIZED
    PSQL_ERROR_MAP.put("N0000", XyzError.EXCEPTION);

    // COLLECTION_EXISTS
    PSQL_ERROR_MAP.put("N0001", XyzError.CONFLICT);

    // COLLECTION_NOT_EXISTS
    PSQL_ERROR_MAP.put("N0002", XyzError.COLLECTION_NOT_FOUND);

    // CHECK_VIOLATION
    PSQL_ERROR_MAP.put("23514", XyzError.EXCEPTION);

    // INVALID_PARAMETER_VALUE
    PSQL_ERROR_MAP.put("22023", XyzError.ILLEGAL_ARGUMENT);

    // UNIQUE_VIOLATION
    PSQL_ERROR_MAP.put("23505", XyzError.CONFLICT);

    // NO_DATA
    PSQL_ERROR_MAP.put("02000", XyzError.NOT_FOUND);
  }

  public static XyzError psqlCodeToXyzError(String psqlCode) {
    return PSQL_ERROR_MAP.getOrDefault(psqlCode, XyzError.get(psqlCode));
  }
}
