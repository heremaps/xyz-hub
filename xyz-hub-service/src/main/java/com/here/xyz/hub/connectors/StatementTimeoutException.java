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

package com.here.xyz.hub.connectors;

import com.here.xyz.util.service.errors.DetailedHttpException;

/**
 * The connector reported that the database cancelled the query, usually because it ran into the statement timeout.
 * The client gets the same 504 (E318540) as for any other connector timeout. The own type only lets the Hub tell this case apart
 * from a connector that did not answer in time, see {@link com.here.xyz.hub.cache.QueryTimeoutCache}.
 */
public class StatementTimeoutException extends DetailedHttpException {

  private static final long serialVersionUID = 1L;

  /**
   * The message of the PSQL connector's TIMEOUT error for SQL states 57014 (query_canceled) and 57P01 (admin_shutdown).
   */
  public static final String CONNECTOR_MESSAGE = "Database query timed out or got canceled.";

  public StatementTimeoutException() {
    super("E318540");
  }
}
