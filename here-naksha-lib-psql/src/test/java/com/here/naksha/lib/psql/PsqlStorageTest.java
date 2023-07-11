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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.ModifyFeaturesReq;
import com.here.naksha.lib.core.storage.ModifyFeaturesResp;
import java.sql.SQLException;
import java.util.Iterator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;

@TestMethodOrder(OrderAnnotation.class)
public class PsqlStorageTest {

  /**
   * The test admin database read from the environment variable with the same name. Value example:
   * {@code jdbc:postgresql://localhost/postgres?user=postgres&password=password&schema=test}
   */
  @SuppressWarnings("unused")
  public static final String TEST_ADMIN_DB = (System.getenv("TEST_ADMIN_DB") != null
          && System.getenv("TEST_ADMIN_DB").length() > "jdbc:postgresql://".length())
      ? System.getenv("TEST_ADMIN_DB")
      : null;

  private boolean isEnabled() {
    return TEST_ADMIN_DB != null;
  }

  static @Nullable PsqlStorage storage;
  static @Nullable PsqlTxWriter tx;

  @Test
  @Order(1)
  @EnabledIf("isEnabled")
  void createStorage() throws Exception {
    final PsqlConfig config = new PsqlConfigBuilder()
        .withAppName("Naksha-Psql-Test")
        .parseUrl(TEST_ADMIN_DB)
        .build();
    storage = new PsqlStorage(config, 0L);
    // This ensures that the upgrade is always done.
    storage.latest = new NakshaVersion(999, 0, 0);
  }

  @Test
  @Order(2)
  @EnabledIf("isEnabled")
  void dropSchemaIfExists() throws Exception {
    final StringBuilder sb = new StringBuilder();
    sb.append("DROP SCHEMA IF EXISTS ");
    assert storage != null;
    PsqlStorage.escapeId(sb, storage.getSchema());
    sb.append(" CASCADE;");
    final String SQL = sb.toString();
    try (final var conn = storage.dataSource.getConnection()) {
      conn.createStatement().execute(SQL);
      conn.commit();
    }
  }

  @Test
  @Order(3)
  @EnabledIf("isEnabled")
  void initStorage() throws Exception {
    assertNotNull(storage);
    storage.init();
  }

  @Test
  @Order(4)
  @EnabledIf("isEnabled")
  void startTransaction() throws Exception {
    assertNotNull(storage);
    tx = storage.openMasterTransaction(storage.createSettings().withAppId("naksha_test"));
    assertNotNull(tx);
  }

  @Test
  @Order(5)
  @EnabledIf("isEnabled")
  void createFooCollection() throws Exception {
    assertNotNull(storage);
    assertNotNull(tx);
    final CollectionInfo collection = tx.createCollection(new CollectionInfo("foo"));
    assertNotNull(collection);
    assertEquals("foo", collection.getId());
    assertTrue(collection.getHistory());
    assertEquals(Long.MAX_VALUE, collection.getMaxAge());
    assertEquals(0L, collection.getDeletedAt());
    tx.commit();
  }

  private static void test_modifyFeatures(@NotNull PsqlTxWriter tx, @NotNull String name, final int count)
      throws SQLException {
    final PsqlFeatureWriter<XyzFeature> writer = tx.writeFeatures(XyzFeature.class, new CollectionInfo("foo"));
    final ModifyFeaturesReq<XyzFeature> req = new ModifyFeaturesReq<>();
    for (int i = 0; i < count; i++) {
      req.insert().add(new XyzFeature(name + i));
    }
    final ModifyFeaturesResp<XyzFeature> resp = writer.modifyFeatures(req);
    tx.commit();
  }

  @Test
  @Order(6)
  @EnabledIf("isEnabled")
  void writeFeaturesIntoFooCollection() throws Exception {
    assertNotNull(storage);
    assertNotNull(tx);
    final String[] prefixes = new String[] {"test", "bar", "fox", "xyz", "lol", "goo", "hey", "kuk"};
    final Thread[] threads = new Thread[prefixes.length];
    for (int i = 0; i < threads.length; i++) {
      final String name = prefixes[i];
      final Thread thread = new Thread(
          () -> {
            assertDoesNotThrow(() -> {
              final var tx = storage.openMasterTransaction(
                  storage.createSettings().withAppId("naksha_test"));
              test_modifyFeatures(tx, name, 10_000);
              tx.commit();
              tx.close();
            });
          },
          name);
      threads[i] = thread;
    }
    for (final var t : threads) {
      t.start();
    }
    for (final var t : threads) {
      t.join();
    }
    //    final PsqlFeatureWriter<XyzFeature> writer = tx.writeFeatures(XyzFeature.class, new
    // CollectionInfo("foo"));
    //    final ModifyFeaturesReq<XyzFeature> req = new ModifyFeaturesReq<>();
    //    for (int i = 0; i < 10000; i++) {
    //      req.insert().add(new XyzFeature("test" + i));
    //    }
    //    final ModifyFeaturesResp<XyzFeature> resp = writer.modifyFeatures(req);
    //    tx.commit();
    //    // assertNotNull(resp);
    //    tx.commit();
  }

  @Test
  @Order(7)
  @EnabledIf("isEnabled")
  void listAllCollections() throws Exception {
    assertNotNull(storage);
    assertNotNull(tx);
    final Iterator<@NotNull CollectionInfo> it = tx.iterateCollections();
    assertTrue(it.hasNext());
    final CollectionInfo collection = it.next();
    assertNotNull(collection);
    assertEquals("foo", collection.getId());
    assertTrue(collection.getHistory());
    assertEquals(Long.MAX_VALUE, collection.getMaxAge());
    assertEquals(0L, collection.getDeletedAt());
    assertFalse(it.hasNext());
  }

  //  @Test
  //  @Order(8)
  //  @EnabledIf("isEnabled")
  //  void dropCollection() throws Exception {
  //    assertNotNull(storage);
  //    assertNotNull(tx);
  //    final CollectionInfo foo = tx.getCollectionById("foo");
  //    assertNotNull(foo);
  //    final CollectionInfo dropped = tx.dropCollection(foo);
  //    tx.commit();
  //    assertNotNull(dropped);
  //    assertNotSame(foo, dropped);
  //    assertEquals(foo.getId(), dropped.getId());
  //    assertEquals(foo.getHistory(), dropped.getHistory());
  //    assertEquals(foo.getMaxAge(), dropped.getMaxAge());
  //    final CollectionInfo fooAgain = tx.getCollectionById("foo");
  //    assertNull(fooAgain);
  //  }

  @EnabledIf("isEnabled")
  @AfterAll
  public static void afterTest() {
    if (tx != null) {
      try {
        tx.close();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        tx = null;
      }
    }
    if (storage != null) {
      try {
        storage.close();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        storage = null;
      }
    }
  }
}
