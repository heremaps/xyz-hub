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
package com.here.naksha.lib.psql.demo;

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.psql.*;
import java.sql.Connection;
import java.sql.Statement;
import org.postgresql.util.PSQLException;

public class CleanInitMain {

  /**
   * To run add argument with url to your DB i.e. "jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=plv_test"
   */
  public static void main(String[] args) {
    if (args.length == 0) {
      System.err.println("Missing argument, please call with JDBC connect string, example: ");
      System.err.println(
          "    jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=plv_test");
      System.exit(1);
      return;
    }
    final PsqlConfig config = new PsqlConfigBuilder()
        .withAppName("Naksha-Psql-Init")
        .parseUrl(args[0])
        .build();
    final PsqlStorage storage = new PsqlStorage(config, config.schema);
    // Connect and initialize the database.
    dropSchema(storage);
    storage.initStorage();
  }

  private static void dropSchema(PsqlStorage storage) {
    try (final Connection conn =
        storage.getDataSource().getPool().dataSource.getConnection()) {
      try (final Statement stmt = conn.createStatement()) {
        try {
          final String sql = "DROP SCHEMA IF EXISTS " + SQL.quote_ident(storage.getSchema()) + " CASCADE";
          stmt.execute(sql);
          stmt.close();
        } catch (PSQLException e) {
          final EPsqlState state = EPsqlState.of(e);
          if (state != EPsqlState.INVALID_SCHEMA_DEFINITION && state != EPsqlState.INVALID_SCHEMA_NAME) {
            throw e;
          }
          conn.rollback();
          currentLogger().info("Naksha schema missing");
        }
        conn.commit();
      }
    } catch (Throwable t) {
      throw unchecked(t);
    }
  }
}
