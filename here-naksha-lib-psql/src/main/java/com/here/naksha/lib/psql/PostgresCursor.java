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

import com.here.naksha.lib.core.util.ClosableChildResource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class PostgresCursor extends ClosableChildResource<PostgresSession> {

  private static final Logger log = LoggerFactory.getLogger(PostgresCursor.class);

  PostgresCursor(
      @NotNull PsqlCursor<?, ?> proxy,
      @NotNull PostgresSession session,
      @NotNull Statement stmt,
      @NotNull ResultSet rs) {
    super(proxy, session);
    this.stmt = stmt;
    this.rs = rs;
  }

  final @NotNull Statement stmt;
  final @NotNull ResultSet rs;

  @Override
  protected void destruct() {
    try {
      rs.close();
    } catch (SQLException e) {
      log.info("Failed to close result-set", e);
    }
    try {
      stmt.close();
    } catch (SQLException e) {
      log.info("Failed to close statement", e);
    }
  }
}
