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
package com.here.naksha.lib.psql.statement;

import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import org.jetbrains.annotations.NotNull;

abstract class StatementCreator {

  private @NotNull Connection connection;
  private @NotNull Duration timeout;

  public StatementCreator(@NotNull Connection connection, @NotNull Duration timeout) {
    this.connection = connection;
    this.timeout = timeout;
  }

  protected @NotNull PreparedStatement preparedStatement(@NotNull String sql) throws SQLException {
    PreparedStatement statement = connection.prepareStatement(sql);
    statement.setQueryTimeout(Math.toIntExact(timeout.getSeconds()));
    return statement;
  }

  protected Array createArrayOf(String type, Object[] elements) throws SQLException {
    return connection.createArrayOf(type, elements);
  }
}
