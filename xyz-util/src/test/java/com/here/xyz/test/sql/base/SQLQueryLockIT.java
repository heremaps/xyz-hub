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

package com.here.xyz.test.sql.base;

import static org.junit.Assert.assertEquals;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.sql.SQLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;

public class SQLQueryLockIT extends SQLITBase {

  private static final String TABLE_NAME = "SQLQueryLockIT";
  private static final String LOCK_KEY = "SQLQueryLockIT_lock";

  @Test
  public void runConcurrentUpdateQueriesWithLock() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropLockTestTable(dsp);
        createLockTestTable(dsp);

        CompletableFuture<Void> f1 = runWriteInThread(SQLQueryLockIT::buildLockedUpdateQuery, dsp);
        CompletableFuture<Void> f2 = runWriteInThread(SQLQueryLockIT::buildLockedUpdateQuery, dsp);

        CompletableFuture.allOf(f1, f2).get();

        assertEquals(1, countRows(dsp));
      }
      finally {
        dropLockTestTable(dsp);
      }
    }
  }

  @Test
  public void runConcurrentReadQueriesWithLock() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropLockTestTable(dsp);
        createLockTestTable(dsp);

        CompletableFuture<Void> f1 = runReadInThread(SQLQueryLockIT::buildLockedReadQuery, dsp);
        CompletableFuture<Void> f2 = runReadInThread(SQLQueryLockIT::buildLockedReadQuery, dsp);

        CompletableFuture.allOf(f1, f2).get();

        assertEquals(1, countRows(dsp));
      }
      finally {
        dropLockTestTable(dsp);
      }
    }
  }

  @Test
  public void runConcurrentBatchUpdateQueriesWithLock() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropLockTestTable(dsp);
        createLockTestTable(dsp);

        CompletableFuture<Void> f1 = runBatchWriteInThread(SQLQueryLockIT::buildLockedBatchUpdateQuery, dsp);
        CompletableFuture<Void> f2 = runBatchWriteInThread(SQLQueryLockIT::buildLockedBatchUpdateQuery, dsp);

        CompletableFuture.allOf(f1, f2).get();

        assertEquals(1, countRows(dsp));
      }
      finally {
        dropLockTestTable(dsp);
      }
    }
  }

  private static SQLQuery buildLockedUpdateQuery() {
    return new SQLQuery("""
        DO $$
        BEGIN
          PERFORM pg_sleep(1);

          IF (SELECT count(1) FROM ${tableName}) = 0 THEN
            INSERT INTO ${tableName} VALUES ('test');
          END IF;
        END$$;
        """)
            .withVariable("tableName", TABLE_NAME)
            .withLock(LOCK_KEY);
  }

  private static SQLQuery buildLockedReadQuery() {
    return new SQLQuery("""
        WITH wait AS (
          SELECT pg_sleep(1)
        ),
        inserted AS (
          INSERT INTO ${tableName} (col)
          SELECT 'test'
          WHERE NOT EXISTS (
            SELECT 1 FROM ${tableName}
          )
          RETURNING col
        )
        SELECT COALESCE(
          (SELECT col FROM inserted),
          (SELECT col FROM ${tableName} LIMIT 1)
        )
        FROM wait;
        """)
            .withVariable("tableName", TABLE_NAME)
            .withLock(LOCK_KEY);
  }

  private static SQLQuery buildLockedBatchUpdateQuery() {
    SQLQuery query = new SQLQuery("SELECT pg_sleep(1);")
            .withLock(LOCK_KEY);

    query.addBatch(new SQLQuery("""
        INSERT INTO ${tableName} (col)
        SELECT 'test'
        WHERE NOT EXISTS (
          SELECT 1 FROM ${tableName}
        );
        """)
            .withVariable("tableName", TABLE_NAME));

    return query;
  }

  private static CompletableFuture<Void> runWriteInThread(Supplier<SQLQuery> querySupplier, DataSourceProvider dsp) {
    return CompletableFuture.runAsync(() -> {
      try {
        querySupplier.get().write(dsp);
      }
      catch (SQLException e) {
        throw new CompletionException(e);
      }
    });
  }

  private static CompletableFuture<Void> runReadInThread(Supplier<SQLQuery> querySupplier, DataSourceProvider dsp) {
    return CompletableFuture.runAsync(() -> {
      try {
        querySupplier.get().run(dsp, rs -> rs.next() ? rs.getString(1) : null);
      }
      catch (SQLException e) {
        throw new CompletionException(e);
      }
    });
  }

  private static CompletableFuture<Void> runBatchWriteInThread(Supplier<SQLQuery> querySupplier, DataSourceProvider dsp) {
    return CompletableFuture.runAsync(() -> {
      try {
        querySupplier.get().writeBatch(dsp);
      }
      catch (SQLException e) {
        throw new CompletionException(e);
      }
    });
  }

  private static int countRows(DataSourceProvider dsp) throws SQLException {
    return new SQLQuery("SELECT count(1) FROM ${tableName}")
            .withVariable("tableName", TABLE_NAME)
            .run(dsp, rs -> rs.next() ? rs.getInt(1) : 0);
  }

  private static void createLockTestTable(DataSourceProvider dsp) throws SQLException {
    new SQLQuery("CREATE TABLE ${tableName} (col TEXT);")
            .withVariable("tableName", TABLE_NAME)
            .write(dsp);
  }

  private static void dropLockTestTable(DataSourceProvider dsp) throws SQLException {
    new SQLQuery("DROP TABLE IF EXISTS ${tableName};")
            .withVariable("tableName", TABLE_NAME)
            .write(dsp);
  }
}