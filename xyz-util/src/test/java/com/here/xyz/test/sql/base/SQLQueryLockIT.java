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


import static com.here.xyz.test.sql.base.SQLITBase.QueryType.BATCH_WRITE;
import static com.here.xyz.test.sql.base.SQLITBase.QueryType.READ;
import static com.here.xyz.test.sql.base.SQLITBase.QueryType.WRITE;
import static com.here.xyz.util.db.pg.LockHelper.buildAdvisoryLockQuery;
import static com.here.xyz.util.db.pg.LockHelper.buildAdvisoryUnlockQuery;
import static org.junit.Assert.assertEquals;

import com.here.xyz.util.db.SQLQuery;
import com.here.xyz.util.db.datasource.DataSourceProvider;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.Test;

public class SQLQueryLockIT extends SQLITBase {

  private static final String LOCK_KEY = "someKey";

  //TODO: Remove that test, once #buildAdvisoryLockQuery() is not used in SQLScript anymore (also then, move that method into SQLQuery)
  @Test
  public void runConcurrentQueriesWithLock() throws Exception {
    SQLQuery concurrentQuery = new SQLQuery("""
        DO $$
        BEGIN
          ${{advisoryLock}}
          PERFORM pg_sleep(1);
          IF (SELECT count(1) FROM ${tableName}) = 0 THEN
            INSERT INTO ${tableName} VALUES ('test');
          END IF;
          ${{advisoryUnlock}}
        END$$;
        """)
        .withVariable("tableName", getDefaultTmpTableName())
        .withQueryFragment("advisoryLock", buildAdvisoryLockQuery(LOCK_KEY))
        .withQueryFragment("advisoryUnlock", buildAdvisoryUnlockQuery(LOCK_KEY));

    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp);
        createTmpTable(dsp);
        CompletableFuture f1 = runQueryInThread(concurrentQuery, dsp, WRITE);
        CompletableFuture f2 = runQueryInThread(concurrentQuery, dsp, WRITE);

        CompletableFuture.allOf(f1, f2).get();

        assertTmpTableSize(dsp, 1);
      }
      finally {
        dropTmpTable(dsp);
      }
    }
  }

  @Test
  public void runConcurrentUpdateQueriesWithLock() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp);
        createTmpTable(dsp);

        CompletableFuture f1 = runQueryInThread(buildLockedUpdateQuery(), dsp, WRITE);
        CompletableFuture f2 = runQueryInThread(buildLockedUpdateQuery(), dsp, WRITE);

        CompletableFuture.allOf(f1, f2).get();

        assertEquals(1, countTmpTableRows(dsp));
      }
      finally {
        dropTmpTable(dsp);
      }
    }
  }

  @Test
  public void runConcurrentReadQueriesWithLock() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp);
        createTmpTable(dsp);

        CompletableFuture f1 = runQueryInThread(buildLockedReadQuery(), dsp, READ);
        CompletableFuture f2 = runQueryInThread(buildLockedReadQuery(), dsp, READ);

        CompletableFuture.allOf(f1, f2).get();

        assertEquals(1, countTmpTableRows(dsp));
      }
      finally {
        dropTmpTable(dsp);
      }
    }
  }

  @Test
  public void runConcurrentBatchUpdateQueriesWithLock() throws Exception {
    try (DataSourceProvider dsp = getDataSourceProvider()) {
      try {
        dropTmpTable(dsp);
        createTmpTable(dsp);

        CompletableFuture f1 = runQueryInThread(buildLockedBatchUpdateQuery(), dsp, BATCH_WRITE);
        CompletableFuture f2 = runQueryInThread(buildLockedBatchUpdateQuery(), dsp, BATCH_WRITE);

        CompletableFuture.allOf(f1, f2).get();

        assertEquals(1, countTmpTableRows(dsp));
      }
      finally {
        dropTmpTable(dsp);
      }
    }
  }

  private SQLQuery buildLockedUpdateQuery() {
    return new SQLQuery("""
        DO $$
        BEGIN
          PERFORM pg_sleep(1);

          IF (SELECT count(1) FROM ${tableName}) = 0 THEN
            INSERT INTO ${tableName} VALUES ('test');
          END IF;
        END$$;
        """)
        .withVariable("tableName", getDefaultTmpTableName())
        .withLock(LOCK_KEY);
  }

  private SQLQuery buildLockedReadQuery() {
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
        .withVariable("tableName", getDefaultTmpTableName())
        .withLock(LOCK_KEY);
  }

  private SQLQuery buildLockedBatchUpdateQuery() {
    SQLQuery query = new SQLQuery("SELECT pg_sleep(1);")
        .withLock(LOCK_KEY);

    query.addBatch(new SQLQuery("""
        INSERT INTO ${tableName} (col)
        SELECT 'test'
        WHERE NOT EXISTS (
          SELECT 1 FROM ${tableName}
        );
        """)
        .withVariable("tableName", getDefaultTmpTableName()));

    return query;
  }
}