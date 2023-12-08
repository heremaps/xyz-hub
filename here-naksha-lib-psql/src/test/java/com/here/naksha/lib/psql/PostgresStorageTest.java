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

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.SQLException;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import org.postgresql.jdbc.PgConnection;

class PostgresStorageTest extends PsqlTests {

  @Override
  boolean enabled() {
    return true;
  }

  @Override
  @NotNull
  String collectionId() {
    return "psql_test";
  }

  @Override
  boolean partition() {
    return false;
  }

  @Test
  void ensureInstanceSingleton() throws SQLException {
    assertNotNull(storage);
    // Get a new connection from the storage.
    PsqlConnection connection = storage.getConnection();
    assertNotNull(connection);
    PostgresConnection postgresConnection = connection.postgresConnection;
    assertNotNull(postgresConnection);
    // Test that we can get the same storage instance again, providing the same config.
    PostgresInstance postgresInstance = postgresConnection.postgresInstance;
    assertNotNull(postgresInstance);
    PsqlInstanceConfig config = postgresInstance.config;
    assertNotNull(config);
    PsqlInstance psqlInstance = PsqlInstance.get(config);
    assertNotNull(psqlInstance);
    assertSame(psqlInstance.postgresInstance, postgresInstance);

    // Remember the underlying pgConnection and then close the connection.
    final PgConnection pgConnection = postgresConnection.get();
    assertNotNull(pgConnection);
    connection.close();
    assertTrue(connection.isClosed());

    // We expect, that the connection was placed into the idle pool.
    connection = storage.getConnection();
    assertNotNull(config);

    // Query a new connection, we should get the very same underlying pgConnection!
    final PgConnection pgConnection2 = connection.postgresConnection.get();
    assertSame(pgConnection, pgConnection2);
    connection.close();
  }
}
