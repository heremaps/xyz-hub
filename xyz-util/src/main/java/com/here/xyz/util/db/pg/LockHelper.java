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

package com.here.xyz.util.db.pg;

import com.here.xyz.util.db.SQLQuery;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class LockHelper {

  private static boolean advisory(String key, Connection connection, boolean lock, boolean block) throws SQLException {
    boolean previousCommitState = connection.getAutoCommit();
    connection.setAutoCommit(true);
    try (Statement stmt = connection.createStatement()) {
      ResultSet rs = stmt.executeQuery(buildLockQuery(key, lock, block, "SELECT").toExecutableQueryString());
      if (!block && rs.next())
        return rs.getBoolean(1);
      return false;
    }
    finally {
      connection.setAutoCommit(previousCommitState);
    }
  }

  private static SQLQuery buildLockQuery(String key, boolean lock, boolean block, String executionKeyWord) {
    return new SQLQuery(executionKeyWord + " pg_" + (lock && !block ? "try_" : "") + "advisory_"
        + (lock ? "" : "un") + "lock(('x' || left(md5('" + key + "'), 15))::bit(60)::bigint);");
  }

  public static SQLQuery buildAdvisoryLockQuery(String key) {
    return buildLockQuery(key, true, true, "PERFORM");
  }

  public static SQLQuery buildAdvisoryUnlockQuery(String key) {
    return buildLockQuery(key, false, true, "PERFORM");
  }

  /**
   * Builds a PL/pgSQL statement that takes a transaction-level advisory lock, which is held until the transaction commits or
   * rolls back. It uses the same key expression as {@link #buildAdvisoryLockQuery(String)}, so it also conflicts with the
   * session-level lock on the same key.
   *
   * @param key The advisory lock key
   * @return The PERFORM statement taking the lock
   */
  public static SQLQuery buildAdvisoryXactLockQuery(String key) {
    return new SQLQuery("PERFORM pg_advisory_xact_lock(('x' || left(md5('" + key.replace("'", "''") + "'), 15))::bit(60)::bigint);");
  }

  public static void advisoryLock(String key, Connection connection) throws SQLException {
    advisory(key, connection, true, true);
  }

  public static void advisoryUnlock(String key, Connection connection) throws SQLException {
    advisory(key, connection, false, true);
  }
}
