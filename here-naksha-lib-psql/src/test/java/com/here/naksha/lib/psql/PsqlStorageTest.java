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
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.models.geojson.implementation.XyzPoint;
import com.here.naksha.lib.core.storage.CollectionInfo;
import com.here.naksha.lib.core.storage.DeleteOp;
import com.here.naksha.lib.core.storage.ModifyFeaturesReq;
import com.here.naksha.lib.core.storage.ModifyFeaturesResp;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;
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

  /**
   * Amount of threads to write features concurrently.
   */
  public static final int THREADS = 10;

  /**
   * Amount of features to write in each thread.
   */
  public static final int MANY_FEATURES_COUNT = 10_000;

  /**
   * If set to true, then the response of the mass-insertion of features is requested.
   */
  public static final boolean READ_RESPONSE = true;

  /**
   * Prevent that the test drops the database at the end (can be used to verify results of write many).
   */
  public static final boolean DO_NOT_DROP = true;

  private boolean isEnabled() {
    return TEST_ADMIN_DB != null;
  }

  static @Nullable PsqlStorage storage;
  static @Nullable PsqlTxWriter tx;
  // Results in ["aaa", "bbb", ...]
  static final String[] prefixes = new String[THREADS];
  static final String[][] ids;
  static final HashMap<String, String[]> idsByPrefix = new HashMap<>();

  static String id(String prefix, int i) {
    return String.format("%s_%06d", prefix, i);
  }

  static {
    ids = new String[THREADS][MANY_FEATURES_COUNT];
    for (int p = 0; p < prefixes.length; p++) {
      final char c = (char) ((int) 'a' + p);
      final String prefix;
      prefixes[p] = prefix = "" + c + c + c;
      final String[] prefixedIds = ids[p];
      for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
        prefixedIds[i] = id(prefix, i);
      }
      idsByPrefix.put(prefix, prefixedIds);
    }
  }

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
    assertNotNull(storage);
    try (final var conn = storage.dataSource.getConnection()) {
      conn.createStatement()
          .execute(new SQL("DROP SCHEMA IF EXISTS ")
              .escape(storage.getSchema())
              .append(" CASCADE;")
              .toString());
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
  void startTransaction() {
    assertNotNull(storage);
    tx = storage.openMasterTransaction(storage.createSettings().withAppId("naksha_test"));
    assertNotNull(tx);
  }

  @Test
  @Order(5)
  @EnabledIf("isEnabled")
  void createFooCollection() {
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

  @Test
  @Order(6)
  @EnabledIf("isEnabled")
  void writeSingleFeatureInFooCollection() {
    assertNotNull(storage);
    assertNotNull(tx);
    final ModifyFeaturesReq<XyzFeature> request = new ModifyFeaturesReq<>(true);
    final XyzFeature single = new XyzFeature("single");
    single.setGeometry(new XyzPoint(5d, 6d));
    request.insert().add(single);
    final ModifyFeaturesResp response =
        tx.writeFeatures(XyzFeature.class, new CollectionInfo("foo")).modifyFeatures(request);
    assertNotNull(response);
    assertEquals(1, response.inserted().size());
    assertEquals(0, response.updated().size());
    assertEquals(0, response.deleted().size());

    final XyzFeature feature = response.inserted().get(0);
    assertNotNull(feature);
    assertEquals("single", feature.getId());
    final XyzGeometry geometry = feature.getGeometry();
    assertNotNull(geometry);
    final XyzPoint point = assertInstanceOf(XyzPoint.class, geometry);
    assertEquals(5d, point.getCoordinates().getLongitude());
    assertEquals(6d, point.getCoordinates().getLatitude());
    assertEquals(0d, point.getCoordinates().getAltitude());
    tx.commit();
  }

  @Test
  @Order(7)
  @EnabledIf("isEnabled")
  void deleteDeleteSingleFeatureFromFooCollection() {
    assertNotNull(storage);
    assertNotNull(tx);
    final ModifyFeaturesReq<XyzFeature> request = new ModifyFeaturesReq<>(true);
    request.delete().add(new DeleteOp("single"));
    final ModifyFeaturesResp response =
        tx.writeFeatures(XyzFeature.class, new CollectionInfo("foo")).modifyFeatures(request);
    assertNotNull(response);
    assertEquals(0, response.inserted().size());
    assertEquals(0, response.updated().size());
    assertEquals(1, response.deleted().size());

    final XyzFeature feature = response.deleted().get(0);
    assertNotNull(feature);
    assertEquals("single", feature.getId());
    final XyzGeometry geometry = feature.getGeometry();
    assertNotNull(geometry);
    final XyzPoint point = assertInstanceOf(XyzPoint.class, geometry);
    assertEquals(5d, point.getCoordinates().getLongitude());
    assertEquals(6d, point.getCoordinates().getLatitude());
    assertEquals(0d, point.getCoordinates().getAltitude());
    tx.commit();
  }

  static class InsertionThread extends Thread {

    InsertionThread(@NotNull String name) {
      super(name);
      this.ids = idsByPrefix.get(name);
      assertNotNull(ids);
      assertEquals(MANY_FEATURES_COUNT, ids.length);
    }

    private final @NotNull String[] ids;
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    @Override
    @SuppressWarnings("SameParameterValue")
    public void run() {
      try {
        final ThreadLocalRandom rand = ThreadLocalRandom.current();
        assertNotNull(storage);
        try (final var tx =
            storage.openMasterTransaction(storage.createSettings().withAppId("naksha_test"))) {
          final PsqlFeatureWriter<XyzFeature> writer =
              tx.writeFeatures(XyzFeature.class, new CollectionInfo("foo"));
          final ModifyFeaturesReq<XyzFeature> req = new ModifyFeaturesReq<>(READ_RESPONSE);
          for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
            final XyzFeature feature = new XyzFeature(ids[i]);
            final double longitude = rand.nextDouble(-180, +180);
            final double latitude = rand.nextDouble(-90, +90);
            final XyzGeometry geometry = new XyzPoint(longitude, latitude);
            feature.setGeometry(geometry);
            req.insert().add(feature);
          }
          final ModifyFeaturesResp response = writer.modifyFeatures(req);
          assertNotNull(response);
          if (READ_RESPONSE) {
            assertEquals(MANY_FEATURES_COUNT, response.inserted().size());
            for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
              final String id = ids[i];
              final XyzFeature feature = response.inserted().get(i);
              assertNotNull(feature);
              assertEquals(id, feature.getId());
            }
          } else {
            assertEquals(0, response.inserted().size());
          }
          assertEquals(0, response.updated().size());
          assertEquals(0, response.deleted().size());
          tx.commit();
        }
      } catch (Exception e) {
        exceptionRef.set(e);
      }
    }
  }

  static class ReadThread extends Thread {

    ReadThread(@NotNull String prefix) {
      super(prefix);
      this.prefix = prefix;
      this.ids = idsByPrefix.get(prefix);
      assertNotNull(ids);
      assertEquals(MANY_FEATURES_COUNT, ids.length);
    }

    private final String prefix;
    private final @NotNull String[] ids;
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    @Override
    @SuppressWarnings("SameParameterValue")
    public void run() {
      try {
        assertNotNull(storage);
        try (final var tx =
            storage.openMasterTransaction(storage.createSettings().withAppId("naksha_test"))) {
          final PsqlFeatureReader<XyzFeature, PsqlTxReader> reader =
              tx.readFeatures(XyzFeature.class, new CollectionInfo("foo"));
          final PsqlResultSet<XyzFeature> rs = reader.getFeaturesById(ids);
          for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
            assertTrue(rs.next());
            assertNotNull(rs.getId());
            assertTrue(rs.getId().startsWith(prefix));
            final XyzFeature feature = rs.getFeature();
            assertNotNull(feature);
            assertEquals(rs.getId(), feature.getId());
          }
        }
      } catch (Exception e) {
        exceptionRef.set(e);
      }
    }
  }

  @Test
  @Order(8)
  @EnabledIf("isEnabled")
  void writeManyFeaturesIntoFooCollection() throws Exception {
    assertNotNull(storage);
    assertNotNull(tx);
    final InsertionThread[] threads = new InsertionThread[prefixes.length];
    for (int i = 0; i < threads.length; i++) {
      final String name = prefixes[i];
      threads[i] = new InsertionThread(name);
    }
    for (final var t : threads) {
      t.start();
    }
    for (final var t : threads) {
      t.join();
      final Exception exception = t.exceptionRef.get();
      if (exception != null) {
        throw exception;
      }
    }
    tx.commit();
  }

  @Test
  @Order(9)
  @EnabledIf("isEnabled")
  void readFeaturesFromFooCollection() throws Exception {
    assertNotNull(tx);
    final ReadThread[] threads = new ReadThread[prefixes.length];
    for (int i = 0; i < threads.length; i++) {
      final String name = prefixes[i];
      threads[i] = new ReadThread(name);
    }
    for (final var t : threads) {
      t.start();
    }
    for (final var t : threads) {
      t.join();
      final Exception exception = t.exceptionRef.get();
      if (exception != null) {
        throw exception;
      }
    }
    tx.commit();
  }

  @Test
  @Order(10)
  @EnabledIf("isEnabled")
  void listAllCollections() {
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

  @Test
  @Order(11)
  @EnabledIf("isEnabled")
  void dropCollection() {
    assertNotNull(storage);
    assertNotNull(tx);
    final CollectionInfo foo = tx.getCollectionById("foo");
    assertNotNull(foo);
    final CollectionInfo dropped = DO_NOT_DROP ? foo : tx.dropCollection(foo);
    tx.commit();
    assertNotNull(dropped);
    if (DO_NOT_DROP) {
      assertSame(foo, dropped);
    } else {
      assertNotSame(foo, dropped);
    }
    assertEquals(foo.getId(), dropped.getId());
    assertEquals(foo.getHistory(), dropped.getHistory());
    assertEquals(foo.getMaxAge(), dropped.getMaxAge());
    final CollectionInfo fooAgain = tx.getCollectionById("foo");
    if (DO_NOT_DROP) {
      assertNotNull(fooAgain);
    } else {
      assertNull(fooAgain);
    }
  }

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
