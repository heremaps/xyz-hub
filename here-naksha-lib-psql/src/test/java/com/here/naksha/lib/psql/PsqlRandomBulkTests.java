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

import static com.here.naksha.lib.core.exceptions.UncheckedException.rethrowExcept;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.SimpleTask;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Future;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;

/**
 * The bulk test.
 */
@SuppressWarnings({"ConstantValue", "RedundantSuppression"})
@TestMethodOrder(OrderAnnotation.class)
public class PsqlRandomBulkTests extends PsqlTests {

  @Override
  boolean enabled() {
    return false;
  }

  final @NotNull String collectionId() {
    return "bulk";
  }

  @Override
  boolean partition() {
    return true;
  }

  /**
   * The amount of features to write for the bulk insert test, zero or less to disable the test.
   */
  static final int BULK_SIZE = 100 * 1000 * 1000;

  /**
   * The amount of parts each thread should handle; set to zero or less to skip bulk test. The value must be maximal 256.
   */
  static final int BULK_PARTS_PER_THREAD = 1;

  String BULK_SCHEMA;

  @Test
  @Order(50)
  @EnabledIf("runTest")
  void prepareStorageForBulk() {
    assertNotNull(storage);
    BULK_SCHEMA = storage.getSchema() + "_tmp";
  }

  @Test
  @Order(60)
  @EnabledIf("runTest")
  void initStorageForBulk() throws Exception {
    assertNotNull(storage);
    assertNotNull(BULK_SCHEMA);
    try (final PsqlSession psqlSession = storage.newWriteSession(null, true)) {
      final PostgresSession session = psqlSession.session();
      SQL sql = session.sql();
      sql.add("CREATE SCHEMA IF NOT EXISTS ").addIdent(BULK_SCHEMA).add(";\n");
      sql.add("SET search_path TO ")
          .addIdent(BULK_SCHEMA)
          .add(',')
          .addIdent(storage.getSchema())
          .add(",toplogoy,public;\n");
      sql.add(
          "CREATE TABLE IF NOT EXISTS test_data (id text, jsondata jsonb, geo geometry, i int8, part_id int);\n");
      sql.add(
          "CREATE UNIQUE INDEX IF NOT EXISTS test_data_id_idx ON test_data USING btree(part_id, id COLLATE \"C\" text_pattern_ops);\n");
      try (PreparedStatement stmt = session.prepareStatement(sql)) {
        stmt.execute();
        session.commit(true);
      }
      int pos = 0;
      while (pos < BULK_SIZE) {
        final int SIZE = Math.min(BULK_SIZE - pos, 10_000_000);
        sql = session.sql();
        sql.add(
                """
WITH bounds AS (SELECT 0 AS origin_x, 0 AS origin_y, 360 AS width, 180 AS height)
INSERT INTO test_data (jsondata, geo, i, id, part_id)
SELECT ('{"id":"'||id||'","properties":{"value":'||((RANDOM() * 100000)::int)||',"@ns:com:here:xyz":{"tags":["'||substr(md5(random()::text),1,1)||'"]}}}')::jsonb,
ST_PointZ(width * (random() - 0.5) + origin_x, height * (random() - 0.5) + origin_y, 0, 4326),
id,
id::text,
nk_head_partition_id(id::text)::int
FROM bounds, generate_series(""")
            .add(pos)
            .add(", ")
            .add(pos + SIZE - 1)
            .add(") id;");
        final String query = sql.toString();
        log.info("Bulk load: " + query);
        pos += SIZE;
        try (PreparedStatement stmt = session.prepareStatement(query)) {
          stmt.execute();
          session.commit(true);
        } catch (Throwable raw) {
          final SQLException e = rethrowExcept(raw, SQLException.class);
          if ("23505".equals(e.getSQLState())) {
            // Duplicate key
            log.info("Values between " + (pos - SIZE) + " and " + pos + " exist already, continue");
          }
          session.rollback(true);
          break;
        }
      }
    }
  }

  final Boolean doBulkWriteRandomData(PsqlStorage storage, int part_id, int parts) {
    try (final PsqlWriteSession session = storage.newWriteSession(null, true)) {
      final PostgresSession pg_session = session.session();
      final SQL sql = pg_session.sql();
      while (parts > 0) {
        final String COLLECTION_PART_NAME = collectionId() + "_p"
            + (part_id < 10 ? "00" + part_id : part_id < 100 ? "0" + part_id : part_id);
        sql.add("INSERT INTO ");
        sql.addIdent(COLLECTION_PART_NAME);
        sql.add(" (jsondata, geo) SELECT jsondata, geo FROM ");
        sql.addIdent(BULK_SCHEMA);
        sql.add(".test_data WHERE part_id = ");
        sql.add(part_id);
        sql.add(";");
        final String query = sql.toString();
        log.info("Bulk insert into partition {}: {}", COLLECTION_PART_NAME, query);
        try (final PreparedStatement stmt = pg_session.prepareStatement(query)) {
          stmt.setFetchSize(1);
          final int inserted = stmt.executeUpdate();
          assertTrue(inserted > 0 && inserted < BULK_SIZE);
          session.commit(true);
        } catch (Exception e) {
          log.error("Failed to bulk load", e);
          return Boolean.FALSE;
        } finally {
          part_id++;
          parts--;
        }
      }
    }
    return Boolean.TRUE;
  }

  @Test
  @Order(70)
  @EnabledIf("runTest")
  void bulkWriteRandomData() throws Exception {
    assertNotNull(storage);
    if (BULK_SIZE > 0 && BULK_PARTS_PER_THREAD > 0) {
      assertTrue(BULK_PARTS_PER_THREAD <= 256);
      final Future<Boolean>[] futures = new Future[256];
      final long START = System.nanoTime();
      int i = 0;
      int startPart = 0;
      while (startPart < 256) {
        // Preparations are done, lets test how long the actual write takes.
        final SimpleTask<Boolean> task = new SimpleTask<>();
        int amount = BULK_PARTS_PER_THREAD;
        if ((startPart + amount) > 256) {
          amount = 256 - startPart;
        }
        futures[i++] = task.start(this::doBulkWriteRandomData, storage, startPart, amount);
        startPart += amount;
      }
      while (--i >= 0) {
        futures[i].get();
      }
      final long END = System.nanoTime();
      printResults("Bulk inserted ", START, END, BULK_SIZE);
    }
  }
}
