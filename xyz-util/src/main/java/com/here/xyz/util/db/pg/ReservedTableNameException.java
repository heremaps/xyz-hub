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
 * A space table name uses the prefix reserved for the empty-space template.
 * It is a server-side inconsistency, not a client error, hence an {@link SQLException} with SQLSTATE XX000.
 */
public class ReservedTableNameException extends SQLException {
  public static final String INTERNAL_ERROR = "XX000";

  public ReservedTableNameException(String table) {
    super("Reserved table name: " + table, INTERNAL_ERROR);
  }
}
