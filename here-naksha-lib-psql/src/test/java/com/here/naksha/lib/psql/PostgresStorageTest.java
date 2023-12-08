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
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.jdbc.PgConnection;

@SuppressWarnings("unused")
@TestMethodOrder(OrderAnnotation.class)
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
  @EnabledIf("runTest")
  @Order(50)
  void ensureInstanceSingleton() throws SQLException {
    assertNotNull(storage, "storage must not be null");
    // Get a new connection from the storage.
    PsqlConnection connection = storage.getConnection();
    assertNotNull(connection, "connection must not be null");
    PostgresConnection postgresConnection = connection.postgresConnection;
    assertNotNull(postgresConnection, "postgresConnection must not be null");
    // Test that we can get the same storage instance again, providing the same config.
    PostgresInstance postgresInstance = postgresConnection.postgresInstance;
    assertNotNull(postgresInstance, "postgresInstance must not be null");
    PsqlInstanceConfig config = postgresInstance.config;
    assertNotNull(config, "config must not be null");
    PsqlInstance psqlInstance = PsqlInstance.get(config);
    assertNotNull(psqlInstance, "We must get back an instance from PsqlInstance.get(config)");
    assertSame(psqlInstance.postgresInstance, postgresInstance, "We expect to get back the same postgres-instance");

    // Remember the underlying pgConnection and then close the connection.
    final PgConnection pgConnection = postgresConnection.get();
    assertNotNull(pgConnection, "We must have a underlying pgConnection");
    connection.close();
    assertTrue(connection.isClosed(), "The connection should be closed");

    // We expect, that the connection was placed into the idle pool.
    connection = storage.getConnection();
    assertNotNull(connection, "The connection must not be null");

    // Query a new connection, we should get the very same underlying pgConnection!
    final PgConnection pgConnection2 = connection.postgresConnection.get();
    assertSame(pgConnection, pgConnection2, "We expect that we get the same underlying pgConnection back again");
    connection.close();
  }
}
