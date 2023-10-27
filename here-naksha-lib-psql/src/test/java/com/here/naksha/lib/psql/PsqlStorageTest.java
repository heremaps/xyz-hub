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

import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.models.geojson.implementation.XyzPoint;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.util.Hex;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.psql.model.XyzFeatureReadResult;

import java.security.SecureRandom;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicReference;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
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
   * The name of the test-collection.
   */
  public static final String COLLECTION_ID = "foo";

  /**
   * Amount of threads to write features concurrently.
   */
  public static final int THREADS = 10;

  /**
   * Amount of features to write in each thread.
   */
  public static final int MANY_FEATURES_COUNT = 10_000;

  /**
   * The amount of times to do the insert.
   */
  public static final int LOOP = 1;
  /**
   * Disable the history for the mass-insertion.
   */
  public static final boolean DISABLE_HISTORY = false;

  /**
   * If set to true, then the response of the mass-insertion of features is requested.
   */
  public static final boolean READ_RESPONSE = true;

  /**
   * Prevents that the test drops the schema at the start, can be used to add more and more features. This has other side effects, like it
   * will not create the collection.
   */
  public static final boolean DROP_INITIALLY = true;

  /**
   * Prevent that the test drops the database at the end (can be used to verify results of write many).
   */
  public static final boolean DROP_FINALLY = false;

  /**
   * If updating should be done.
   */
  public static final boolean DO_UPDATE = true;

  record UpdateKey(String key, String[] values) {
  }

  public static final UpdateKey[] UPDATE_KEYS = new UpdateKey[]{
      new UpdateKey("name", new String[]{null, "Michael Schmidt", "Thomas Bar", "Alexander Foo"})
  };

  /**
   * Tests if the environment variable "TEST_ADMIN_DB" is set.
   */
  private boolean isEnabled() {
    return TEST_ADMIN_DB != null;
  }

  private boolean dropInitially() {
    return isEnabled() && DROP_INITIALLY;
  }

  private boolean dropFinally() {
    return isEnabled() && DROP_FINALLY;
  }

  private boolean doUpdate() {
    return isEnabled() && DO_UPDATE;
  }

  static @Nullable PsqlStorage storage;
  static @Nullable PsqlWriteSession session;
  // Results in ["aaa", "bbb", ...]
  static String[] prefixes = new String[THREADS];
  static String[][] ids;
  static String[] tags;
  static HashMap<String, String[]> idsByPrefix = new HashMap<>();

  static String id(String prefix, int i) {
    return String.format("%s_%06d", prefix, i);
  }

  static void initStatics(boolean first) {
    final SecureRandom rand = new SecureRandom();
    final StringBuilder sb = new StringBuilder();
    for (int i = 0; i < 20; i++) {
      sb.append(Hex.valueToChar[rand.nextInt(Hex.valueToChar.length)]);
    }
    sb.append("_");
    final String super_prefix = sb.toString();
    ids = new String[THREADS][MANY_FEATURES_COUNT];
    for (int p = 0; p < prefixes.length; p++) {
      // Use always the same identifiers aaa_<i>.
      final char c = (char) ((int) 'a' + p);
      final String prefix = (first ? "" : super_prefix) + c + c + c;
      prefixes[p] = prefix;
      final String[] prefixedIds = ids[p];
      for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
        prefixedIds[i] = id(prefix, i);
      }
      idsByPrefix.put(prefix, prefixedIds);
    }
    tags = new String[Math.max(10, MANY_FEATURES_COUNT / 1000)];
    for (int i = 0; i < tags.length; i++) {
      tags[i] = "tag_" + i;
    }
  }

  @BeforeAll
  static void beforeTest() {
    NakshaContext.currentContext().setAuthor("PsqlStorageTest");
    NakshaContext.currentContext().setAppId("naksha-lib-psql-unit-tests");
    initStatics(DROP_INITIALLY);
  }

  @Test
  @Order(10)
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
  @Order(20)
  @EnabledIf("dropInitially")
  void dropSchemaIfExists() throws Exception {
    assertNotNull(storage);
    try (final var conn = storage.dataSource.getPool().dataSource.getConnection()) {
      conn.createStatement()
          .execute(new SQL("DROP SCHEMA IF EXISTS ")
              .add_ident(storage.getSchema())
              .append(" CASCADE;")
              .toString());
      conn.commit();
    }
  }

  @Test
  @Order(30)
  @EnabledIf("isEnabled")
  void initStorage() throws Exception {
    assertNotNull(storage);
    storage.init();
  }

  @Test
  @Order(40)
  @EnabledIf("isEnabled")
  void startTransaction() throws SQLException {
    assertNotNull(storage);
    session = new PsqlWriteSession(storage, storage.dataSource.getConnection());
    assertNotNull(session);
  }

  @Test
  @Order(50)
  @EnabledIf("dropInitially")
  void createCollection() {
    assertNotNull(storage);
    assertNotNull(session);
    StorageCollection storageCollection = new StorageCollection(COLLECTION_ID);
    WriteOp<StorageCollection> writeOp = new WriteOp<>(EWriteOp.INSERT, storageCollection, false);
    WriteCollections<StorageCollection> writeRequest = new WriteCollections<>(List.of(writeOp));
    final WriteResult<StorageCollection> result = (WriteResult<StorageCollection>) session.execute(writeRequest);
    session.commit();
    assertNotNull(result);
    assertEquals(1, result.results.size());
    assertEquals(EExecutedOp.INSERTED, result.results.get(0).op);
    assertEquals(COLLECTION_ID, result.results.get(0).object.getId());
    assertTrue(result.results.get(0).object.hasHistory());
    assertEquals(Long.MAX_VALUE, result.results.get(0).object.getMaxAge());
    assertEquals(0L, result.results.get(0).object.getDeletedAt());
    session.commit();
  }

  @Test
  @Order(60)
  @EnabledIf("isEnabled")
  void writeSingleFeature() {
    assertNotNull(storage);
    assertNotNull(session);
    NakshaFeature nakshaFeature = new NakshaFeature("featureId_1");
    nakshaFeature.setGeometry(new XyzPoint(5d, 6d, 2d));
    WriteFeatures writeFeaturesRequest = RequestHelper.createFeatureRequest(COLLECTION_ID, nakshaFeature);
    final WriteResult<NakshaFeature> result = (WriteResult<NakshaFeature>) session.execute(writeFeaturesRequest);
    assertNotNull(result);
    List<WriteOpResult<NakshaFeature>> results = result.results;
    assertEquals(1, count(results, EExecutedOp.INSERTED));
    assertEquals(0, count(results, EExecutedOp.UPDATED));
    assertEquals(0, count(results, EExecutedOp.DELETED));

    final NakshaFeature feature = results.get(0).object;
    assertNotNull(feature);
    assertEquals("featureId_1", feature.getId());
    final XyzGeometry geometry = feature.getGeometry();
    assertNotNull(geometry);
    final XyzPoint point = assertInstanceOf(XyzPoint.class, geometry);
    assertEquals(5d, point.getCoordinates().getLongitude());
    assertEquals(6d, point.getCoordinates().getLatitude());
    assertEquals(2d, point.getCoordinates().getAltitude());
    session.commit();
  }

  @Test
  @Order(70)
  @EnabledIf("isEnabled")
  void deleteSingleFeature() {
    assertNotNull(storage);
    assertNotNull(session);
    NakshaFeature nakshaFeature = new NakshaFeature("featureId_1");
    WriteFeatures writeFeaturesRequest =
        RequestHelper.createFeatureRequest(COLLECTION_ID, nakshaFeature, IfExists.DELETE, IfConflict.DELETE);
    final WriteResult<NakshaFeature> result = (WriteResult<NakshaFeature>) session.execute(writeFeaturesRequest);
    List<WriteOpResult<NakshaFeature>> results = result.results;
    assertNotNull(result);
    assertEquals(0, count(results, EExecutedOp.INSERTED));
    assertEquals(1, count(results, EExecutedOp.UPDATED));
    assertEquals(0, count(results, EExecutedOp.DELETED));

    final NakshaFeature feature = results.get(0).object;
    assertNotNull(feature);
    assertEquals("featureId_1", feature.getId());
    session.commit();
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
        final String[] allTags = PsqlStorageTest.tags;
        final ThreadLocalRandom rand = ThreadLocalRandom.current();
        assertNotNull(storage);
        try (final var tx =
                 storage.openMasterTransaction(storage.createSettings().withAppId("naksha_test"))) {

          final List<NakshaFeature> featuresToWrite = new ArrayList<>(MANY_FEATURES_COUNT);
          for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
            final NakshaFeature feature = new NakshaFeature(ids[i]);
            final double longitude = rand.nextDouble(-180, +180);
            final double latitude = rand.nextDouble(-90, +90);
            final XyzGeometry geometry = new XyzPoint(longitude, latitude);
            feature.setGeometry(geometry);

            // 25% to get one tag
            int tag_i = rand.nextInt(0, allTags.length << 1);
            if (tag_i < allTags.length) {
              final XyzNamespace ns = feature.getProperties().getXyzNamespace();
              final ArrayList<String> tags = new ArrayList<>();
              for (int j = 0; j < 3 && tag_i < allTags.length; j++) {
                final String tag = allTags[tag_i];
                if (!tags.contains(tag)) {
                  tags.add(tag);
                }
                // 50% chance to get one tag, 25% change to get two, ~6% change to get three.
                tag_i = rand.nextInt(0, allTags.length << 1);
              }
              ns.setTags(tags, false);
            }
            featuresToWrite.add(feature);
          }
          WriteFeatures writeFeaturesRequest =
              RequestHelper.createFeatureRequest(COLLECTION_ID, featuresToWrite, false);

          final WriteResult<NakshaFeature> writeResult =
              (WriteResult<NakshaFeature>) session.execute(writeFeaturesRequest);
          List<WriteOpResult<NakshaFeature>> results = writeResult.results;
          assertNotNull(writeResult);
          long insertCount = count(results, EExecutedOp.INSERTED);
          if (READ_RESPONSE) {
            assertEquals(MANY_FEATURES_COUNT, insertCount);
            for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
              final String id = ids[i];
              final NakshaFeature feature = results.get(i).object;
              assertNotNull(feature);
              assertEquals(id, feature.getId());
            }
          } else {
            assertEquals(0, insertCount);
          }
          assertEquals(0, count(results, EExecutedOp.INSERTED));
          assertEquals(0, count(results, EExecutedOp.DELETED));
          tx.commit();
        }
      } catch (Exception e) {
        exceptionRef.set(e);
      }
    }
  }

  static class UpdateThread extends Thread {

    UpdateThread(@NotNull String prefix) {
      super(prefix);
      this.prefix = prefix;
      String[] ids = idsByPrefix.get(prefix);
      assertNotNull(ids);
      assertEquals(MANY_FEATURES_COUNT, ids.length);
    }

    private final @NotNull String prefix;
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    @Override
    @SuppressWarnings("SameParameterValue")
    public void run() {
      try {
        assertNotNull(storage);
        final SecureRandom rand = new SecureRandom();
        final ConcurrentHashMap<String, NakshaFeature> featuresById = readFeaturesByPrefix.get(prefix);
        assertNotNull(featuresById);
        assertEquals(MANY_FEATURES_COUNT, featuresById.size());
        try (final var tx =
                 storage.openMasterTransaction(storage.createSettings().withAppId("naksha_test"))) {
          final List<NakshaFeature> featuresToUpdate = new ArrayList<>(MANY_FEATURES_COUNT);

          final ArrayList<String> updateIds = new ArrayList<>();
          {
            final Enumeration<String> idsEnum = featuresById.keys();
            int i = 0;
            while (idsEnum.hasMoreElements()) {
              final String id = idsEnum.nextElement();
              assertNotNull(id);
              assertFalse(updateIds.contains(id));
              updateIds.add(id);
              final NakshaFeature feature = featuresById.get(id);
              assertNotNull(feature);
              if (++i > MANY_FEATURES_COUNT) {
                throw new IllegalStateException("There must be not more than " + MANY_FEATURES_COUNT
                    + " features for prefix" + prefix);
              }
              List<@NotNull String> tags =
                  feature.getProperties().getXyzNamespace().getTags();
              if (tags == null) {
                tags = new ArrayList<>();
                feature.getProperties().getXyzNamespace().setTags(tags, false);
                tags.add(prefix);
              } else if (!tags.contains(prefix)) {
                tags.add(prefix);
              }
              for (final UpdateKey updateKey : UPDATE_KEYS) {
                final String value = updateKey.values[rand.nextInt(updateKey.values.length)];
                if (value != null) {
                  feature.getProperties().put(updateKey.key, value);
                } else {
                  feature.getProperties().remove(updateKey.key);
                }
              }
              featuresToUpdate.add(feature);
            }
          }
          WriteFeatures writeFeaturesRequest = RequestHelper.createFeaturesRequest(
              COLLECTION_ID, featuresToUpdate, IfExists.REPLACE, IfConflict.REPLACE);
          final WriteResult<NakshaFeature> writeResult =
              (WriteResult<NakshaFeature>) session.execute(writeFeaturesRequest);
          assertNotNull(writeResult);
          List<WriteOpResult<NakshaFeature>> results = writeResult.results;
          if (READ_RESPONSE) {
            assertEquals(MANY_FEATURES_COUNT, count(results, EExecutedOp.UPDATED));
            for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
              final String id = updateIds.get(i);
              assertNotNull(id);
              final NakshaFeature feature = results.get(i).object;
              assertNotNull(feature);
              assertEquals(id, feature.getId());
            }
          } else {
            assertEquals(0, count(results, EExecutedOp.UPDATED));
          }
          assertEquals(0, count(results, EExecutedOp.INSERTED));
          assertEquals(0, count(results, EExecutedOp.DELETED));
          tx.commit();
        }
      } catch (Exception e) {
        exceptionRef.set(e);
      }
    }
  }

  static ConcurrentHashMap<String, ConcurrentHashMap<String, NakshaFeature>> readFeaturesByPrefix =
      new ConcurrentHashMap<>();
  static ConcurrentHashMap<String, XyzFeature> readFeaturesById = new ConcurrentHashMap<>();

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
        final ConcurrentHashMap<String, NakshaFeature> features = new ConcurrentHashMap<>();
        readFeaturesByPrefix.put(prefix, features);

        ReadFeatures readFeatures = RequestHelper.readFeaturesByIdsRequest(COLLECTION_ID, List.of(ids));

        XyzFeatureReadResult readResult = (XyzFeatureReadResult) session.execute(readFeatures);
        for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
          assertTrue(readResult.hasNext());
          XyzFeature xyzFeature = readResult.next();
          assertNotNull(xyzFeature);
          assertNotNull(xyzFeature.getId());
          assertTrue(xyzFeature.getId().startsWith(prefix));
          readFeaturesById.put(xyzFeature.getId(), xyzFeature);
        }
      } catch (Exception e) {
        exceptionRef.set(e);
      }
    }
  }

  @Test
  @Order(80)
  @EnabledIf("isEnabled")
  void writeManyFeatures() throws Exception {
    assertNotNull(storage);
    assertNotNull(session);
    try {
      if (DISABLE_HISTORY) {
        //        session.disableHistory(new CollectionInfo(COLLECTION_ID));
        //        session.commit();
      }
      long total = 0L;
      for (int loop = 0; loop < LOOP; loop++) {
        if (loop > 0) {
          if (DROP_INITIALLY) {
            System.out.flush();
            System.err.flush();
            System.err.println("WARNING: REPEAT option not supported when DROP_INITIALLY is false!");
            System.out.flush();
            System.err.flush();
            break;
          }
          // Generates new identifiers for us.
          initStatics(false);
        }
        final long START = System.nanoTime();
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
        session.commit();
        printResults("Inserted", START, System.nanoTime());
        total += THREADS * MANY_FEATURES_COUNT;
        System.out.printf("\tTotal: %,d\n", total);
        System.out.flush();
      }
      if (DISABLE_HISTORY) {
        //        session.enableHistory(new CollectionInfo(COLLECTION_ID));
        //        session.commit();
      }
    } finally {
      if (!DROP_INITIALLY) {
        initStatics(true);
      }
    }
  }

  @Test
  @Order(90)
  @EnabledIf("isEnabled")
  void readFeatures() throws Exception {
    assertNotNull(session);
    final long START = System.nanoTime();
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
    session.commit();
    printResults("Read", START, System.nanoTime());
  }

  @Test
  @Order(100)
  @EnabledIf("doUpdate")
  void updateManyFeatures() throws Exception {
    assertNotNull(storage);
    assertNotNull(session);
    if (DISABLE_HISTORY) {
      //      session.disableHistory(new CollectionInfo(COLLECTION_ID));
      //      session.commit();
    }
    final long START = System.nanoTime();
    assertEquals(THREADS * MANY_FEATURES_COUNT, readFeaturesById.size());
    final UpdateThread[] threads = new UpdateThread[prefixes.length];
    for (int i = 0; i < threads.length; i++) {
      final String prefix = prefixes[i];
      threads[i] = new UpdateThread(prefix);
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
    session.commit();
    printResults("Updated", START, System.nanoTime());
    if (DISABLE_HISTORY) {
      //      session.enableHistory(new CollectionInfo(COLLECTION_ID));
      //      session.commit();
    }
  }

  private void printResults(final String prefix, final long START, final long END) {
    final long NANOS = END - START;
    final long FEATURES = MANY_FEATURES_COUNT * THREADS;
    final double MS = NANOS / 1_000_000d;
    final double SECONDS = MS / 1_000d;
    final double FEATURES_PER_SECOND = FEATURES / SECONDS;
    System.out.printf(
        "%s %,d features in %2.2f seconds, %6.2f features/seconds\n",
        prefix, FEATURES, SECONDS, FEATURES_PER_SECOND);
    System.out.flush();
  }

  @Test
  @Order(110)
  @EnabledIf("isEnabled")
  void listAllCollections() throws SQLException {
    assertNotNull(storage);
    assertNotNull(session);
    ReadCollections readCollections =
        new ReadCollections().withReadDeleted(true).withIds(COLLECTION_ID);
    XyzFeatureReadResult<StorageCollection> readResult =
        (XyzFeatureReadResult<StorageCollection>) session.execute(readCollections);
    assertTrue(readResult.hasNext());
    final StorageCollection collection = readResult.next();
    assertNotNull(collection);
    assertEquals(COLLECTION_ID, collection.getId());
    //    assertTrue(collection.getHistory());
    assertEquals(Long.MAX_VALUE, collection.getMaxAge());
    assertEquals(0L, collection.getDeletedAt());
    assertFalse(readResult.hasNext());
  }

  @Test
  @Order(120)
  @EnabledIf("isEnabled")
  void dropFooCollection() {
    assertNotNull(storage);
    assertNotNull(session);
    StorageCollection storageCollection = new StorageCollection(COLLECTION_ID);
    WriteOp<StorageCollection> writeOp = new WriteOp<>(EWriteOp.DELETE, storageCollection, false);
    WriteCollections<StorageCollection> writeRequest = new WriteCollections<>(List.of(writeOp));
    final WriteResult<StorageCollection> dropResult =
        (WriteResult<StorageCollection>) session.execute(writeRequest);
    session.commit();
    assertNotNull(dropResult);
    StorageCollection dropped = dropResult.results.get(0).object;
    if (dropFinally()) {
      assertNotSame(storageCollection, dropped);
    } else {
      assertNotSame(storageCollection, dropped);
    }
    assertEquals(storageCollection.getId(), dropped.getId());
    //    assertEquals(storageCollection.getHistory(), dropped.getHistory());
    assertEquals(storageCollection.getMaxAge(), dropped.getMaxAge());
    ReadCollections readRequest =
        new ReadCollections().withIds(COLLECTION_ID).withReadDeleted(false);
    XyzFeatureReadResult result = (XyzFeatureReadResult) session.execute(readRequest);
    if (dropFinally()) {
      assertFalse(result.hasNext());
    } else {
      assertTrue(result.hasNext());
    }
  }

  @EnabledIf("isEnabled")
  @AfterAll
  public static void afterTest() {
    if (session != null) {
      try {
        session.close();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        session = null;
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

  private static <T> long count(List<WriteOpResult<T>> results, EExecutedOp type) {
    return results.stream().filter(op -> op.op.equals(type)).count();
  }
}
