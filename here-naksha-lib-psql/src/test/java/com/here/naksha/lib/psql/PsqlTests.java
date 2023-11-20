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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.EExecutedOp;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.here.naksha.lib.core.models.storage.WriteXyzCollections;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all PostgresQL tests that require some test database.
 */
@TestMethodOrder(OrderAnnotation.class)
abstract class PsqlTests {

  static final Logger log = LoggerFactory.getLogger(PsqlTests.class);

  /**
   * The test admin database read from the environment variable with the same name. Value example:
   * <pre>{@code
   * jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=test&app=test&id=test
   * }</pre>
   */
  @SuppressWarnings("unused")
  static final String TEST_DB =
      (System.getenv("TEST_DB") != null && System.getenv("TEST_DB").length() > "jdbc:postgresql://".length())
          ? System.getenv("TEST_DB")
          : null;

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
    return TEST_DB != null && enabled();
  }

  final boolean dropInitially() {
    return runTest() && DROP_INITIALLY;
  }

  final boolean dropFinally() {
    return runTest() && DROP_FINALLY;
  }

  static final String TEST_APP_ID = "test_app";
  static final String TEST_AUTHOR = "test_author";
  static @Nullable PsqlStorage storage;
  static @Nullable NakshaContext nakshaContext;
  static @Nullable PsqlWriteSession session;

  /**
   * Prints an arbitrary prefix, followed by the calculation of the features/second.
   *
   * @param prefix   The arbitrary prefix to print.
   * @param START    The start time in nanoseconds.
   * @param END      The end time in nanoseconds.
   * @param features The number features effected.
   */
  static void printResults(final @NotNull String prefix, final long START, final long END, final long features) {
    final long NANOS = END - START;
    final double MS = NANOS / 1_000_000d;
    final double SECONDS = MS / 1_000d;
    final double FEATURES_PER_SECOND = features / SECONDS;
    log.info(String.format(
        "%s %,d features in %2.2f seconds, %6.2f features/seconds\n",
        prefix, features, SECONDS, FEATURES_PER_SECOND));
  }

  @BeforeAll
  static void beforeTest() {
    NakshaContext.currentContext().setAuthor("PsqlStorageTest");
    NakshaContext.currentContext().setAppId("naksha-lib-psql-unit-tests");
    nakshaContext = new NakshaContext().withAppId(TEST_APP_ID).withAuthor(TEST_AUTHOR);
  }

  /**
   * The name of the test-collection.
   */
  abstract @NotNull String collectionId();

  /**
   * The test-schema to use, can be overridden to switch the schema.
   */
  @NotNull
  String schema() {
    assertNotNull(schema);
    return schema;
  }

  private String schema;

  @Test
  @Order(10)
  @EnabledIf("runTest")
  void createStorage() {
    storage = new PsqlStorage(TEST_DB);
    schema = storage.getSchema();
    if (!schema.equals(schema())) {
      storage.setSchema(schema());
    }
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
    storage.initStorage();
  }

  @Test
  @Order(20)
  @EnabledIf("runTest")
  void startWriteSession() {
    assertNotNull(storage);
    session = storage.newWriteSession(nakshaContext, true);
    assertNotNull(session);
  }

  @Test
  @Order(30)
  @EnabledIf("runTest")
  void createCollection() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzCollections request = new WriteXyzCollections();
    request.add(EWriteOp.CREATE, new XyzCollection(collectionId(), true, false, true));
    try (final ForwardCursor<XyzCollection, XyzCollectionCodec> cursor =
        session.execute(request).getXyzCollectionCursor()) {
      assertNotNull(cursor);
      assertTrue(cursor.hasNext());
      assertTrue(cursor.next());
      assertEquals(collectionId(), cursor.getId());
      assertNotNull(cursor.getUuid());
      assertNull(cursor.getGeometry());
      assertSame(EExecutedOp.CREATED, cursor.getOp());
      final XyzCollection collection = cursor.getFeature();
      assertNotNull(collection);
      assertEquals(collectionId(), collection.getId());
      assertFalse(collection.pointsOnly());
      assertTrue(collection.isPartitioned());
      assertEquals(64, collection.partitionCount());
      assertNotNull(collection.getProperties());
      assertNotNull(collection.getProperties().getXyzNamespace());
      assertSame(
          EXyzAction.CREATE,
          collection.getProperties().getXyzNamespace().getAction());
      assertFalse(cursor.hasNext());
    } finally {
      session.commit(true);
    }
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
