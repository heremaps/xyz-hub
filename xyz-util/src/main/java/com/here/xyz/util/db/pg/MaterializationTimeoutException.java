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

package com.here.xyz.util.db.pg;

import java.sql.SQLException;

/**
 * The materialization of a lazy space table did not finish within its budget. Retrying is safe.
 * The SQLSTATE is {@value #LOCK_NOT_AVAILABLE} for a lock wait or {@value #QUERY_CANCELED} for a DDL or JDBC timeout
 * or a budget that was too small to start.
 */
public class MaterializationTimeoutException extends SQLException {
  public static final String LOCK_NOT_AVAILABLE = "55P03";
  public static final String QUERY_CANCELED = "57014";

  public enum Phase {
    /**
     * Too little budget left to start, no database access happened
     */
    BUDGET,
    /**
     * Waiting for the lock of the table
     */
    LOCK,
    /**
     * Running the DDL statements
     */
    DDL
  }

  private final Phase phase;

  public MaterializationTimeoutException(String reason, String sqlState, Phase phase, Throwable cause) {
    super(reason, sqlState, cause);
    this.phase = phase;
  }

  public Phase getPhase() {
    return phase;
  }
}
