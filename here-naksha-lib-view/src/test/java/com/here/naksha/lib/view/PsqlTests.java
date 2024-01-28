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
package com.here.naksha.lib.view;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.psql.*;
import com.here.naksha.lib.psql.PsqlStorage.Params;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.here.naksha.lib.psql.PsqlStorageConfig.configFromFileOrEnv;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Base class for all PostgresQL tests that require some test database.
 */
@SuppressWarnings("unused")
@TestMethodOrder(OrderAnnotation.class)
abstract class PsqlTests {

  static final Logger log = LoggerFactory.getLogger(PsqlTests.class);

  /**
   * The test database, if any is available.
   */
  @SuppressWarnings("unused")
  static final @NotNull PsqlStorageConfig config =
          configFromFileOrEnv("test_psql_db.url", "NAKSHA_TEST_PSQL_DB_URL", "naksha_view_test_schema");

  /**
   * Prevents that the test drops the schema at the start.
   */
  static final boolean DROP_INITIALLY = true;

  /**
   * If the test drop the database at the end (false by default, to verify results).
   */
  static final boolean DROP_FINALLY = false;

  /**
   * Logging level.
   */
  static final EPsqlLogLevel LOG_LEVEL = EPsqlLogLevel.VERBOSE;

  abstract boolean enabled();

  final boolean runTest() {
    return enabled();
  }

  abstract boolean dropInitially();

  abstract boolean dropFinally();

  static final String TEST_APP_ID = "test_app";
  static final String TEST_AUTHOR = "test_author";
  static @Nullable PsqlStorage storage;
  static @Nullable NakshaContext nakshaContext;
  static @Nullable PsqlWriteSession session;
  static @NotNull PsqlFeatureGenerator fg;

  @BeforeAll
  static void beforeTest() {
    NakshaContext.currentContext().setAuthor("PsqlStorageTest");
    NakshaContext.currentContext().setAppId("naksha-lib-view-unit-tests");
    nakshaContext = new NakshaContext().withAppId(TEST_APP_ID).withAuthor(TEST_AUTHOR);
    fg = new PsqlFeatureGenerator();
  }

  private String schema;
  /**
   * The test-schema to use, can be overridden to switch the schema.
   */
  @NotNull
  String schema() {
    assertNotNull(schema);
    return schema;
  }

  @Test
  @Order(10)
  @EnabledIf("runTest")
  void createStorage() {
    storage = new PsqlStorage(config);
    schema = storage.getSchema();
    if (!schema.equals(schema())) {
      storage.setSchema(schema());
    }
    // Enable this code line to get debug output from the database!
    // storage.setLogLevel(EPsqlLogLevel.DEBUG);
  }

  @Test
  @Order(11)
  @EnabledIf("dropInitially")
  void dropSchemaIfExists() {
    assertNotNull(storage);
    storage.dropSchema();
  }

  @Test
  @Order(12)
  @EnabledIf("runTest")
  void initStorage() {
    assertNotNull(storage);
    storage.initStorage(new Params().pg_hint_plan(false).pg_stat_statements(false));
  }

  @Test
  @Order(13)
  @EnabledIf("runTest")
  void startWriteSession() {
    assertNotNull(storage);
    session = storage.newWriteSession(nakshaContext, true);
    assertNotNull(session);
  }

  // Custom stuff between 50 and 9000

  @Test
  @Order(9999)
  @EnabledIf("dropFinally")
  void dropSchemaFinally() {
    assertNotNull(storage);
    storage.dropSchema();
  }

  @EnabledIf("runTest")
  @AfterAll
  static void afterTest() {
    if (session != null) {
      try {
        session.close();
      } catch (Exception e) {
        log.atError()
            .setMessage("Failed to close write-session")
            .setCause(e)
            .log();
      } finally {
        session = null;
      }
    }
    if (storage != null) {
      try {
        storage.close();
      } catch (Exception e) {
        log.atError().setMessage("Failed to close storage").setCause(e).log();
      } finally {
        storage = null;
      }
    }
  }
}
