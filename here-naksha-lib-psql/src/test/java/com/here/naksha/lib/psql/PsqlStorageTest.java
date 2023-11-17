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
import static com.spatial4j.core.io.GeohashUtils.encodeLatLon;
import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.SimpleTask;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.coordinates.MultiPointCoordinates;
import com.here.naksha.lib.core.models.geojson.implementation.*;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IWriteSession;
import com.here.naksha.lib.core.util.Hex;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import java.security.SecureRandom;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.condition.DisabledIf;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({"CallToPrintStackTrace", "unchecked", "unused"})
@TestMethodOrder(OrderAnnotation.class)
public class PsqlStorageTest {

  private static final Logger log = LoggerFactory.getLogger(PsqlStorageTest.class);

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
   * Logging level.
   */
  public static final EPsqlLogLevel LOG_LEVEL = EPsqlLogLevel.VERBOSE;

  /**
   * The name of the test-collection.
   */
  public static final String COLLECTION_ID = "foo";

  /**
   * The amount of features to write for the bulk insert test, zero or less to disable the test.
   */
  public static final int BULK_SIZE = 100 * 1000 * 1000;

  /**
   * The amount of parts each thread should handle; set to zero or less to skip bulk test. The value must be maximal 256.
   */
  public static final int BULK_PARTS_PER_THREAD = 1;

  /**
   * Amount of threads to write features concurrently.
   */
  @Deprecated
  public static final int THREADS = 10;

  /**
   * Amount of features to write in each thread.
   */
  @Deprecated
  public static final int MANY_FEATURES_COUNT = 10_000;

  /**
   * The amount of times to do the insert.
   */
  @Deprecated
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

  @Deprecated
  record UpdateKey(String key, String[] values) {

  }

  public static final UpdateKey[] UPDATE_KEYS = new UpdateKey[]{
      new UpdateKey("name", new String[]{null, "Michael Schmidt", "Thomas Bar", "Alexander Foo"})
  };

  /**
   * Tests if the environment variable "TEST_ADMIN_DB" is set.
   */
  private boolean hasTestDb() {
    return TEST_ADMIN_DB != null;
  }

  @SuppressWarnings("ConstantValue")
  private boolean doBulk() {
    return hasTestDb() && BULK_SIZE > 0 && BULK_PARTS_PER_THREAD > 0;
  }

  // This is only to disable tests not yet working!
  @Deprecated
  private boolean isTrue() {
    return true;
  }

  private boolean dropInitially() {
    return hasTestDb() && DROP_INITIALLY;
  }

  private boolean dropFinally() {
    return hasTestDb() && DROP_FINALLY;
  }

  private boolean doUpdate() {
    return hasTestDb() && DO_UPDATE;
  }

  static final String TEST_APP_ID = "test_app";
  static final String TEST_AUTHOR = "test_author";
  static final String TEST_STORAGE_ID = "test";
  static @Nullable PsqlStorage storage;
  static @Nullable NakshaContext nakshaContext;
  static @Nullable PsqlWriteSession session;
  // Results in ["aaa", "bbb", ...]
  @Deprecated
  static String[] prefixes = new String[THREADS];

  @Deprecated
  static String[][] ids;

  @Deprecated
  static String[] tags;

  @Deprecated
  static HashMap<String, String[]> idsByPrefix = new HashMap<>();

  @Deprecated
  static String id(String prefix, int i) {
    return String.format("%s_%06d", prefix, i);
  }

  @Deprecated
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
    nakshaContext = new NakshaContext().withAppId(TEST_APP_ID).withAuthor(TEST_AUTHOR);
  }

  @Test
  @Order(10)
  @EnabledIf("hasTestDb")
  void createStorage() throws Exception {
    final PsqlStorageConfigBuilder builder = new PsqlStorageConfigBuilder()
        .withAppName("Naksha-Psql-Test")
        .parseUrl(TEST_ADMIN_DB)
        .withStatementTimeout(1, TimeUnit.HOURS);
    if (doBulk()) {
      if (LOG_LEVEL.toLong() > 0) {
        log.error("Log level " + LOG_LEVEL + " is not allowed for bulk load, reduce to "+EPsqlLogLevel.OFF);
        builder.withLogLevel(EPsqlLogLevel.OFF);
      } else {
        builder.withLogLevel(LOG_LEVEL);
      }
    }
    final PsqlStorageConfig config = builder.build();
    storage = new PsqlStorage(config, TEST_STORAGE_ID);
  }

  @Test
  @Order(20)
  @EnabledIf("dropInitially")
  void dropSchemaIfExists() {
    assertNotNull(storage);
    storage.dropSchema();
  }

  @Test
  @Order(30)
  @EnabledIf("hasTestDb")
  void initStorage() {
    assertNotNull(storage);
    storage.initStorage();
  }

  @Test
  @Order(31)
  @EnabledIf("doBulk")
  void initStorageForBulk() throws Exception {
    assertNotNull(storage);
    BULK_SCHEMA = storage.getSchema() + "_tmp";
    try (PsqlSession psqlSession = storage.newWriteSession(null, true)) {
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
      try (PreparedStatement stmt = session.prepareWithCursor(sql)) {
        stmt.execute();
        session.connection.commit();
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
        try (PreparedStatement stmt = session.prepareWithCursor(query)) {
          stmt.execute();
          session.connection.commit();
        } catch (Throwable raw) {
          final SQLException e = rethrowExcept(raw, SQLException.class);
          if ("23505".equals(e.getSQLState())) {
            // Duplicate key
            log.info("Values between " + (pos - SIZE) + " and " + pos + " exist already, continue");
          }
          session.connection.rollback();
          break;
        }
      }
    }
  }

  private static String BULK_SCHEMA;

  @Test
  @Order(40)
  @EnabledIf("hasTestDb")
  void startTransaction() throws SQLException {
    assertNotNull(storage);
    session = storage.newWriteSession(nakshaContext, true);
    assertNotNull(session);
  }

  @Test
  @Order(50)
  @EnabledIf("dropInitially")
  void createCollection() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzCollections<XyzCollection> request = new WriteXyzCollections<>();
    request.add(new WriteXyzOp<>(EWriteOp.CREATE, new XyzCollection(COLLECTION_ID, true, false, true)));
    try (final ResultCursor<XyzCollection> cursor = session.execute(request).cursor(XyzCollection.class)) {
      assertNotNull(cursor);
      assertTrue(cursor.hasNext());
      assertTrue(cursor.next());
      assertEquals(COLLECTION_ID, cursor.getId());
      assertNotNull(cursor.getUuid());
      assertNull(cursor.getGeometry());
      assertSame(EExecutedOp.CREATED, cursor.getOp());
      final XyzCollection collection = cursor.getFeature();
      assertNotNull(collection);
      assertEquals(COLLECTION_ID, collection.getId());
      assertFalse(collection.pointsOnly());
      assertTrue(collection.isPartitioned());
      assertEquals(64, collection.partitionCount());
      assertNotNull(collection.getProperties());
      assertNotNull(collection.getProperties().getXyzNamespace());
      assertSame(
          EXyzAction.CREATE,
          collection.getProperties().getXyzNamespace().getAction());
      assertFalse(cursor.next());
    } finally {
      session.commit();
    }
  }

  static final String SINGLE_FEATURE_ID = "TheFeature";

  @Test
  @Order(60)
  @EnabledIf("hasTestDb")
  void singleFeatureCreate() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzFeatures<XyzFeature> request = new WriteXyzFeatures<>(COLLECTION_ID);
    final XyzFeature feature = new XyzFeature(SINGLE_FEATURE_ID);
    feature.setGeometry(new XyzPoint(5.0d, 6.0d, 2.0d));
    request.add(new WriteXyzOp<>(EWriteOp.CREATE, feature));
    try (final ResultCursor<XyzFeature> cursor = session.execute(request).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
      final EExecutedOp op = cursor.getOp();
      assertSame(EExecutedOp.CREATED, op);
      final String id = cursor.getId();
      assertEquals(SINGLE_FEATURE_ID, id);
      final String uuid = cursor.getUuid();
      assertNotNull(uuid);
      final Geometry geometry = cursor.getGeometry();
      assertNotNull(geometry);
      final Coordinate coordinate = geometry.getCoordinate();
      assertEquals(5.0d, coordinate.getOrdinate(0));
      assertEquals(6.0d, coordinate.getOrdinate(1));
      assertEquals(2.0d, coordinate.getOrdinate(2));
      final XyzFeature f = cursor.getFeature();
      assertNotNull(f);
      assertEquals(SINGLE_FEATURE_ID, f.getId());
      assertEquals(uuid, f.xyz().getUuid());
      assertSame(EXyzAction.CREATE, f.xyz().getAction());
      assertFalse(cursor.next());
    } finally {
      session.commit();
    }
  }

  @Test
  @Order(61)
  @EnabledIf("hasTestDb")
  void singleFeatureCreateVerify() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    // given
    /**
     * data inserted in {@link #singleFeatureCreate()} test
     */
    final ReadFeatures request = RequestHelper.readFeaturesByIdRequest(COLLECTION_ID, SINGLE_FEATURE_ID);

    // when
    try (final ResultCursor<XyzFeature> cursor = session.execute(request).cursor()) {
      cursor.next();
      final XyzFeature feature = cursor.getFeature();
      XyzNamespace xyz = feature.xyz();

      // then
      assertEquals(SINGLE_FEATURE_ID, feature.getId());
      assertEquals(1, xyz.getVersion());
      assertSame(EXyzAction.CREATE, xyz.getAction());
      final XyzGeometry geometry = feature.getGeometry();
      assertNotNull(geometry);
      final Coordinate coordinate = geometry.getJTSGeometry().getCoordinate();
      assertEquals(5.0d, coordinate.getOrdinate(0));
      assertEquals(6.0d, coordinate.getOrdinate(1));
      assertEquals(2.0d, coordinate.getOrdinate(2));

      final String uuid = cursor.getUuid();
      assertEquals(cursor.getUuid(), xyz.uuid);
      final String[] uuidFields = uuid.split(":");
      assertEquals(TEST_STORAGE_ID, uuidFields[0]);
      assertEquals(COLLECTION_ID, uuidFields[1]);
      assertEquals(4, uuidFields[2].length()); // year (4- digits)
      assertEquals(2, uuidFields[3].length()); // hour (2- digits)
      assertEquals(2, uuidFields[4].length()); // minute (2- digits)
      assertEquals("1", uuidFields[5]); // seq id
      final String txnFromUuid = uuidFields[2] + uuidFields[3] + uuidFields[4] + "0000000000" + uuidFields[5];
      assertEquals(txnFromUuid, xyz.getTxn().toString()); // seq id
      // FIXME new write session should prepare context on DB.
      // FIXME new write session should prepare context on DB.
      //      assertEquals(TEST_APP_ID, xyz.getAppId());
      //      assertEquals(TEST_AUTHOR, xyz.getAuthor());
      assertNotEquals(xyz.getRealTimeCreateAt(), xyz.getCreatedAt());
      assertEquals(xyz.getCreatedAt(), xyz.getUpdatedAt());

      assertEquals(encodeLatLon(coordinate.y, coordinate.x, 7), xyz.get("grid"));

      assertFalse(cursor.next());
      cursor.beforeFirst();
      assertTrue(cursor.next());
    }
  }

  @Test
  @Order(62)
  @EnabledIf("hasTestDb")
  void singleFeatureUpdate() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    // given
    /**
     * data inserted in {@link #singleFeatureCreate()} test
     */
    final NakshaFeature featureToUpdate = new NakshaFeature(SINGLE_FEATURE_ID);
    // different geometry
    XyzPoint newPoint1 = new XyzPoint(5.1d, 6.0d, 2.1d);
    XyzPoint newPoint2 = new XyzPoint(5.15d, 6.0d, 2.15d);
    XyzMultiPoint multiPoint = new XyzMultiPoint();
    MultiPointCoordinates multiPointCoordinates = new MultiPointCoordinates(2);
    multiPointCoordinates.add(0, newPoint1.getCoordinates());
    multiPointCoordinates.add(0, newPoint2.getCoordinates());
    multiPoint.withCoordinates(multiPointCoordinates);

    featureToUpdate.setGeometry(multiPoint);
    // new property added
    featureToUpdate.setTitle("Bank");
    final WriteFeatures<NakshaFeature> request = RequestHelper.updateFeatureRequest(COLLECTION_ID, featureToUpdate);

    // when
    try (final ResultCursor<NakshaFeature> cursor = session.execute(request).cursor()) {
      cursor.next();

      // then
      final XyzFeature feature = cursor.getFeature(XyzFeature.class);
      assertSame(EExecutedOp.UPDATED, cursor.getOp());
      assertEquals(SINGLE_FEATURE_ID, cursor.getId());
      // FIXME should properties type be filled?
      //      assertNotNull(cursor.getPropertiesType());
      final Geometry coordinate = cursor.getGeometry();
      assertEquals(multiPoint.convertToJTSGeometry(), coordinate);
      // FIXME we should be able to read features just by cursor.getFeature(NakshaFeature.class)
      assertEquals("Bank", feature.get("title").toString());
    }
  }

  @Test
  @Order(63)
  @EnabledIf("hasTestDb")
  void singleFeatureUpdateVerify() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    // given
    /**
     * data inserted in {@link #singleFeatureCreate()} test and updated by {@link #singleFeatureUpdate()}.
     */
    final ReadFeatures request = RequestHelper.readFeaturesByIdRequest(COLLECTION_ID, SINGLE_FEATURE_ID);

    // when
    try (final ResultCursor<XyzFeature> cursor = session.execute(request).cursor()) {
      cursor.next();
      final XyzFeature feature = cursor.getFeature();
      XyzNamespace xyz = feature.xyz();

      // then
      assertEquals(SINGLE_FEATURE_ID, feature.getId());
      assertEquals(2, xyz.getVersion());
      assertSame(EXyzAction.UPDATE, xyz.getAction());
      final XyzGeometry geometry = feature.getGeometry();
      assertNotNull(geometry);
      Coordinate expectedGeometry = new Coordinate(5.15d, 6.0d, 2.15d);
      assertEquals(expectedGeometry, geometry.getJTSGeometry().getCoordinate());

      final String uuid = cursor.getUuid();
      assertEquals(cursor.getUuid(), xyz.uuid);
      final String[] uuidFields = uuid.split(":");
      assertEquals(TEST_STORAGE_ID, uuidFields[0]);
      assertEquals(COLLECTION_ID, uuidFields[1]);
      assertEquals(4, uuidFields[2].length()); // year (4- digits)
      assertEquals(2, uuidFields[3].length()); // hour (2- digits)
      assertEquals(2, uuidFields[4].length()); // minute (2- digits)
      // should have next seq id, which is 2:
      assertEquals("2", uuidFields[5]); // seq id
      final String txnFromUuid = uuidFields[2] + uuidFields[3] + uuidFields[4] + "0000000000" + uuidFields[5];
      assertEquals(txnFromUuid, xyz.getTxn().toString()); // seq id
      // FIXME new write session should prepare context on DB.
      // FIXME new write session should prepare context on DB.
      //      assertEquals(TEST_APP_ID, xyz.getAppId());
      //      assertEquals(TEST_AUTHOR, xyz.getAuthor());

      Point centroid = geometry.getJTSGeometry().getCentroid();
      assertEquals(encodeLatLon(centroid.getY(), centroid.getX(), 7), xyz.get("grid"));
      assertFalse(cursor.next());
    }
  }

  @Test
  @Order(64)
  @EnabledIf("hasTestDb")
  void singleFeatureDelete() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzFeatures<XyzFeature> request = new WriteXyzFeatures<>(COLLECTION_ID);
    final XyzFeature feature = new XyzFeature(SINGLE_FEATURE_ID);
    feature.setGeometry(new XyzPoint(5.0d, 6.0d, 2.0d));
    request.add(new WriteXyzOp<>(EWriteOp.DELETE, feature));
    try (final ResultCursor<XyzFeature> cursor = session.execute(request).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
      final EExecutedOp op = cursor.getOp();
      assertSame(EExecutedOp.DELETED, op);
      final String id = cursor.getId();
      assertEquals(SINGLE_FEATURE_ID, id);
      final String uuid = cursor.getUuid();
      assertNotNull(uuid);
      final Geometry geometry = cursor.getGeometry();
      assertNotNull(geometry);
      final Coordinate coordinate = geometry.getCoordinate();
      assertEquals(5.15d, coordinate.getOrdinate(0));
      assertEquals(6.0d, coordinate.getOrdinate(1));
      assertEquals(2.15d, coordinate.getOrdinate(2));
      final XyzFeature f = cursor.getFeature();
      assertNotNull(f);
      assertEquals(SINGLE_FEATURE_ID, f.getId());
      assertEquals(uuid, f.xyz().getUuid());
      assertSame(EXyzAction.DELETE, f.xyz().getAction());
      assertFalse(cursor.next());
    } finally {
      session.commit();
    }
  }

  @Test
  @Order(65)
  @EnabledIf("hasTestDb")
  void singleFeatureDeleteVerify() throws NoCursor, SQLException {
    assertNotNull(storage);
    assertNotNull(session);

    // when
    /**
     * Read from feature should return nothing.
     */
    final ReadFeatures request = RequestHelper.readFeaturesByIdRequest(COLLECTION_ID, SINGLE_FEATURE_ID);
    try (final ResultCursor<XyzFeature> cursor = session.execute(request).cursor()) {
      assertFalse(cursor.hasNext());
    }
    // also: direct query to feature table should return nothing.
    try (final PsqlReadSession session = storage.newReadSession(null, true)) {
      ResultSet rs = getFeatureFromTable(session, COLLECTION_ID, SINGLE_FEATURE_ID);
      assertFalse(rs.next());
    }

    /**
     * Read from deleted should return valid feature.
     */
    final ReadFeatures requestWithDeleted = RequestHelper.readFeaturesByIdRequest(COLLECTION_ID, SINGLE_FEATURE_ID);
    requestWithDeleted.withReturnDeleted(true);
    String featureJsonBeforeDeletion;

    /* TODO uncomment it when read with deleted is ready.

    try (final ResultCursor<XyzFeature> cursor =
    session.execute(requestWithDeleted).cursor()) {
    cursor.next();
    final XyzFeature feature = cursor.getFeature();
    XyzNamespace xyz = feature.xyz();

    // then
    assertSame(EExecutedOp.DELETED, cursor.getOp());
    final String id = cursor.getId();
    assertEquals(SINGLE_FEATURE_ID, id);
    final String uuid = cursor.getUuid();
    assertNotNull(uuid);
    final Geometry geometry = cursor.getGeometry();
    assertNotNull(geometry);
    assertEquals(new Coordinate(5.1d, 6.0d, 2.1d), geometry.getCoordinate());
    assertNotNull(feature);
    assertEquals(SINGLE_FEATURE_ID, feature.getId());
    assertEquals(uuid, feature.xyz().getUuid());
    assertSame(EXyzAction.DELETE, feature.xyz().getAction());
    featureJsonBeforeDeletion = cursor.getJson()
    assertFalse(cursor.next());
    }
    */
    /**
     * Check directly _del table.
     */
    final String collectionDelTableName = COLLECTION_ID + "_del";
    try (final PsqlReadSession session = storage.newReadSession(null, true)) {
      ResultSet rs = getFeatureFromTable(session, collectionDelTableName, SINGLE_FEATURE_ID);

      // feature exists in _del table
      assertTrue(rs.next());

      /* FIXME uncomment this when read with deleted is ready.
      assertEquals(featureJsonBeforeDeletion, rs.getString(1));
       */
    }
  }

  @Test
  @Order(66)
  @EnabledIf("hasTestDb")
  void singleFeaturePurge() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    // given
    /**
     * Data inserted in {@link #singleFeatureCreate()} and deleted in {@link #singleFeatureDelete()}.
     * We don't care about geometry or other properties during PURGE operation, feature_id is only required thing,
     * thanks to that you don't have to read feature before purge operation.
     */
    final XyzFeature featureToPurge = new XyzFeature(SINGLE_FEATURE_ID);
    final WriteXyzFeatures<XyzFeature> request = new WriteXyzFeatures<>(COLLECTION_ID);
    request.add(new WriteXyzOp<>(EWriteOp.PURGE, featureToPurge));

    // when
    try (final ResultCursor<XyzFeature> cursor = session.execute(request).cursor()) {
      cursor.next();

      // then
      final XyzFeature feature = cursor.getFeature(XyzFeature.class);
      assertSame(EExecutedOp.PURGED, cursor.getOp());
      assertEquals(SINGLE_FEATURE_ID, cursor.getId());
    } finally {
      session.commit();
    }
  }

  @Test
  @Order(67)
  @EnabledIf("hasTestDb")
  void singleFeaturePurgeVerify() throws NoCursor, SQLException {
    assertNotNull(storage);
    assertNotNull(session);

    // given
    final String collectionDelTableName = COLLECTION_ID + "_del";

    // when
    try (final PsqlReadSession session = storage.newReadSession(null, true)) {
      ResultSet rs = getFeatureFromTable(session, collectionDelTableName, SINGLE_FEATURE_ID);

      // then
      assertFalse(rs.next());
    }
  }

  @Deprecated
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
          long insertCount = count(results, EExecutedOp.CREATED);
          if (READ_RESPONSE) {
            assertEquals(MANY_FEATURES_COUNT, insertCount);
            for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
              final String id = ids[i];
              final NakshaFeature feature = results.get(i).feature;
              assertNotNull(feature);
              assertEquals(id, feature.getId());
            }
          } else {
            assertEquals(0, insertCount);
          }
          assertEquals(0, count(results, EExecutedOp.CREATED));
          assertEquals(0, count(results, EExecutedOp.DELETED));
          tx.commit();
        }
      } catch (Exception e) {
        exceptionRef.set(e);
      }
    }
  }

  @Deprecated
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
              final NakshaFeature feature = results.get(i).feature;
              assertNotNull(feature);
              assertEquals(id, feature.getId());
            }
          } else {
            assertEquals(0, count(results, EExecutedOp.UPDATED));
          }
          assertEquals(0, count(results, EExecutedOp.CREATED));
          assertEquals(0, count(results, EExecutedOp.DELETED));
          tx.commit();
        }
      } catch (Exception e) {
        exceptionRef.set(e);
      }
    }
  }

  @Deprecated
  static ConcurrentHashMap<String, ConcurrentHashMap<String, NakshaFeature>> readFeaturesByPrefix =
      new ConcurrentHashMap<>();

  @Deprecated
  static ConcurrentHashMap<String, XyzFeature> readFeaturesById = new ConcurrentHashMap<>();

  @Deprecated
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

        //        XyzFeatureReadResult readResult = (XyzFeatureReadResult) session.execute(readFeatures);
        //        for (int i = 0; i < MANY_FEATURES_COUNT; i++) {
        //          assertTrue(readResult.hasNext());
        //          XyzFeature xyzFeature = readResult.next();
        //          assertNotNull(xyzFeature);
        //          assertNotNull(xyzFeature.getId());
        //          assertTrue(xyzFeature.getId().startsWith(prefix));
        //          readFeaturesById.put(xyzFeature.getId(), xyzFeature);
        //        }
      } catch (Exception e) {
        exceptionRef.set(e);
      }
    }
  }

  @Test
  @Order(80)
  @EnabledIf("hasTestDb")
  @DisabledIf("isTrue")
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

  private static Boolean bulkWriteRandomData__(PsqlStorage storage, int part_id, int parts) {
    try (final PsqlWriteSession session = storage.newWriteSession(null, true)) {
      final PostgresSession pgSession = session.session();
      final SQL sql = pgSession.sql();
      while (parts > 0) {
        final String COLLECTION_PART_NAME = COLLECTION_ID + "_p"
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
        try (final PreparedStatement stmt = pgSession.prepareWithoutCursor(query)) {
          stmt.setFetchSize(1);
          final int inserted = stmt.executeUpdate();
          assertTrue(inserted > 0 && inserted < BULK_SIZE);
          session.commit();
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
  @Order(81)
  @EnabledIf("hasTestDb")
  void bulkWriteRandomData() throws Exception {
    assertNotNull(storage);
    if (BULK_SIZE > 0 && BULK_PARTS_PER_THREAD > 0) {
      //noinspection ConstantValue
      assertTrue(BULK_PARTS_PER_THREAD <= 256);
      final Future<Boolean>[] futures = new Future[256];
      final long START = System.nanoTime();
      int i = 0;
      int startPart = 0;
      while (startPart < 256) {
        // Preparations are done, lets test how long the actual write takes.
        final SimpleTask<Boolean> task = new SimpleTask<>();
        int amount = BULK_PARTS_PER_THREAD;
        //noinspection ConstantValue
        if ((startPart + amount) > 256) {
          amount = 256 - startPart;
        }
        futures[i++] = task.start(PsqlStorageTest::bulkWriteRandomData__, storage, startPart, amount);
        startPart += amount;
      }
      while (--i >= 0) {
        futures[i].get();
      }
      final long END = System.nanoTime();
      // Show results.
      final long NANOS = END - START;
      final double MS = NANOS / 1_000_000d;
      final double SECONDS = MS / 1_000d;
      final double FEATURES_PER_SECOND = BULK_SIZE / SECONDS;
      log.info(String.format(
          "%,d features in %2.2f seconds, %6.2f features/seconds\n",
          BULK_SIZE, SECONDS, FEATURES_PER_SECOND));
    }
  }

  @Test
  @Order(82)
  @EnabledIf("hasTestDb")
  @DisabledIf("isTrue")
  @Deprecated
  void bulkWrite() throws Exception {
    assertNotNull(storage);
    if (BULK_SIZE > 0) {
      final SecureRandom rand = new SecureRandom();
      final String[] allTags = new String[Math.max(10, BULK_SIZE / 1000)];
      for (int i = 0; i < allTags.length; i++) {
        allTags[i] = "tag_" + i;
      }
      final ConcurrentHashMap<String, XyzFeature> allFeatures = new ConcurrentHashMap<>();
      final ArrayList<WriteOp<XyzFeature>> ops = new ArrayList<>();
      for (int i = 0; i < BULK_SIZE; i++) {
        final String featureId = RandomStringUtils.randomAlphabetic(20);
        final XyzFeature feature = new XyzFeature(featureId);
        final double longitude = rand.nextDouble(-180, +180);
        final double latitude = rand.nextDouble(-90, +90);
        final XyzGeometry geometry = new XyzPoint(longitude, latitude);
        feature.setGeometry(geometry);
        // 25% to get one tag
        //        int tag_i = rand.nextInt(0, allTags.length << 1);
        //        if (tag_i < allTags.length) {
        //          final ArrayList<String> tags = new ArrayList<>();
        //          for (int j = 0; j < 3 && tag_i < allTags.length; j++) {
        //            final String tag = allTags[tag_i];
        //            if (!tags.contains(tag)) {
        //              tags.add(tag);
        //            }
        //            // 50% chance to get one tag, 25% change to get two, ~6% change to get three.
        //            tag_i = rand.nextInt(0, allTags.length << 1);
        //          }
        //          feature.xyz().setTags(tags, false);
        //        }
        ops.add(new WriteXyzOp<>(EWriteOp.PUT, feature));
        assertFalse(allFeatures.containsKey(featureId));
        allFeatures.put(featureId, feature);
      }
      final WriteXyzFeatures<XyzFeature> writeRequest = new WriteXyzFeatures<>(COLLECTION_ID, ops);
      writeRequest.minResults = true;
      final List<@NotNull WriteFeatures<XyzFeature>> bulkWrites = null; // storage.newBulkSession(writeRequest);
      final ConcurrentHashMap<SimpleTask<Result>, Future<Result>> resultFutures = new ConcurrentHashMap<>();

      // Preparations are done, lets test how long the actual write takes.
      final long START = System.nanoTime();
      int i = 0;
      for (final @NotNull WriteFeatures<XyzFeature> request : bulkWrites) {
        final SimpleTask<Result> task = new SimpleTask<>(Integer.toString(i++));
        final Future<Result> future = task.start(
            (req) -> {
              System.out.println("Write " + req.features.size() + " features in task #" + task.id());
              final IWriteSession session = storage.newWriteSession(null, true);
              NakshaContext.currentContext().with(task.id(), session);
              Result result;
              try {
                result = session.execute(req);
                try (final ResultCursor<XyzFeature> cursor = result.getXyzFeatureCursor()) {
                  while (cursor.next()) {
                    final String featureId = cursor.getId();
                    assertTrue(allFeatures.containsKey(featureId));
                    allFeatures.remove(featureId);
                    break;
                  }
                } catch (NoCursor e) {
                  if (e.result instanceof ErrorResult err) {
                    System.out.println(err.reason + " " + err.message);
                    if (err.exception != null) {
                      err.exception.printStackTrace();
                    }
                  } else {
                    System.out.println("WTF?");
                    e.printStackTrace();
                  }
                }
                session.commit();
              } catch (Exception e) {
                result = new ErrorResult(XyzError.EXCEPTION, e.getMessage(), e);
              } finally {
                session.close();
              }
              return result;
            },
            request);
        resultFutures.put(task, future);
      }
      // At this point all writes should run concurrently, lets wait for all results and commit them.
      Enumeration<SimpleTask<Result>> keyEnum = resultFutures.keys();
      while (keyEnum.hasMoreElements()) {
        final SimpleTask<Result> task = keyEnum.nextElement();
        assertNotNull(task);
        final Future<Result> resultFuture = resultFutures.get(task);
        assertNotNull(resultFuture);
        final Result result = assertDoesNotThrow(() -> resultFuture.get());
        assertNotNull(result);
      }
      // At this point the import is done.
      final long END = System.nanoTime();

      // Show results.
      final long NANOS = END - START;
      final double MS = NANOS / 1_000_000d;
      final double SECONDS = MS / 1_000d;
      final double FEATURES_PER_SECOND = BULK_SIZE / SECONDS;
      System.out.printf(
          "%,d features in %2.2f seconds, %6.2f features/seconds\n", BULK_SIZE, SECONDS, FEATURES_PER_SECOND);
      System.out.flush();
      // assertEquals(0, allFeatures.size());
      System.out.println("Unread features: " + allFeatures.size());
    }
  }

  @Test
  @Order(90)
  @EnabledIf("hasTestDb")
  @DisabledIf("isTrue")
  @Deprecated
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
  @DisabledIf("isTrue")
  @Deprecated
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

  @Deprecated
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
  @EnabledIf("hasTestDb")
  @DisabledIf("isTrue")
  @Deprecated
  void listAllCollections() throws SQLException {
    assertNotNull(storage);
    assertNotNull(session);
    //    ReadCollections readCollections =
    //        new ReadCollections().withReadDeleted(true).withIds(COLLECTION_ID);
    //    XyzFeatureReadResult<StorageCollection> readResult =
    //        (XyzFeatureReadResult<StorageCollection>) session.execute(readCollections);
    //    assertTrue(readResult.hasNext());
    //    final StorageCollection collection = readResult.next();
    //    assertNotNull(collection);
    //    assertEquals(COLLECTION_ID, collection.getId());
    //    //    assertTrue(collection.getHistory());
    //    assertEquals(Long.MAX_VALUE, collection.getMaxAge());
    //    assertEquals(0L, collection.getDeletedAt());
    //    assertFalse(readResult.hasNext());
  }

  @Test
  @Order(120)
  @EnabledIf("hasTestDb")
  void dropFooCollection() throws NoCursor, SQLException {
    assertNotNull(storage);
    assertNotNull(session);

    final WriteCollections<NakshaFeature> deleteRequest = new WriteCollections<>();

    final NakshaFeature feature = new NakshaFeature(COLLECTION_ID);
    deleteRequest.add(new WriteOp<>(EWriteOp.DELETE, COLLECTION_ID, null, feature, null, true));
    try (final ResultCursor<XyzFeature> cursor =
        session.execute(deleteRequest).cursor()) {
      assertTrue(cursor.next());
      session.commit();

      PsqlReadSession readDeletedSession = storage.newReadSession(nakshaContext, false);
      ResultSet readRs = getFeatureFromTable(readDeletedSession, COLLECTION_ID, SINGLE_FEATURE_ID);
      // It should not have any data but table still exists.
      assertFalse(readRs.next());
      readDeletedSession.close();

      // purge
      final WriteCollections<NakshaFeature> purgeRequest = new WriteCollections<>();
      purgeRequest.add(new WriteOp<>(EWriteOp.PURGE, COLLECTION_ID, null, feature, null, true));
      try (final ResultCursor<XyzFeature> cursorPurge =
          session.execute(purgeRequest).cursor()) {
        session.commit();
      }

      // try readSession after purge, table doesn't exist anymore, so it should throw an exception.
      PsqlReadSession readPurgedSession = storage.newReadSession(nakshaContext, false);
      assertThrowsExactly(
          PSQLException.class,
          () -> getFeatureFromTable(readPurgedSession, COLLECTION_ID, SINGLE_FEATURE_ID),
          "ERROR: relation \"foo\" does not exist");
      readPurgedSession.close();
    }
  }

  @EnabledIf("hasTestDb")
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

  @Deprecated
  private static <T> long count(List<WriteOpResult<T>> results, EExecutedOp type) {
    return results.stream().filter(op -> op.op.equals(type)).count();
  }

  private ResultSet getFeatureFromTable(PsqlReadSession session, String table, String featureId) throws SQLException {
    final PostgresSession pgSession = session.session();
    final SQL sql = pgSession.sql().add("SELECT * from ").addIdent(table).add(" WHERE jsondata->>'id' = ? ;");
    final PreparedStatement stmt = pgSession.prepare(sql);
    stmt.setString(1, featureId);
    return stmt.executeQuery();
  }
}
