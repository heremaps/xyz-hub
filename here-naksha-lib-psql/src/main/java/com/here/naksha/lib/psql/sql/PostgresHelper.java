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
package com.here.naksha.lib.psql.sql;

import com.here.naksha.lib.core.util.Unsafe;
import com.zaxxer.hikari.pool.ProxyConnection;
import java.lang.reflect.Field;
import java.sql.Connection;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A set of static methods that assist in handling communication with PostgresQL database.
 */
final class PostgresHelper {
  static final Logger log = LoggerFactory.getLogger(PostgresHelper.class);

  private static final long delegate_OFFSET;

  static {
    try {
      final Field delegate = ProxyConnection.class.getDeclaredField("delegate");
      //noinspection deprecation
      delegate_OFFSET = Unsafe.unsafe.objectFieldOffset(delegate);
    } catch (Throwable t) {
      throw new Error(t);
    }
  }

  /**
   * Tries to extract the {@link PGConnection} from the given connection proxy.
   *
   * @param connection The connection to extract the {@link PGConnection} from.
   * @return either the extracted {@link PGConnection} of {@code null}, if the connection is no {@link PGConnection} or can't be extracted.
   */
  static @Nullable PGConnection pgConnection(@NotNull Connection connection) {
    // Note:
    // dataSource is a PsqlDataSource
    // dataSource.pool is a PsqlPool
    // dataSource.pool.dataSource is a HikariDataSource
    // dataSource.pool.dataSource.pool is a HikariPool
    // If this pool eventually is asked for a connection is uses the ProxyFactory (from Hikari library)
    // It will return a ProxyConnection (from Hikari library)
    // This ProxyConnection has a property "protected Connection delegate;"
    // Which eventually is a PGConnection
    if (connection instanceof PGConnection pgConn) {
      return pgConn;
    }
    if (connection instanceof ProxyConnection proxyConnection) {
      return (PGConnection) Unsafe.unsafe.getObject(proxyConnection, delegate_OFFSET);
    }
    return null;
  }
}
