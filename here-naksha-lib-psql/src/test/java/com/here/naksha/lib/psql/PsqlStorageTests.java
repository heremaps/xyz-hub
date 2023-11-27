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

import static com.spatial4j.core.io.GeohashUtils.encodeLatLon;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.geojson.coordinates.JTSHelper;
import com.here.naksha.lib.core.models.geojson.coordinates.LineStringCoordinates;
import com.here.naksha.lib.core.models.geojson.coordinates.MultiPointCoordinates;
import com.here.naksha.lib.core.models.geojson.coordinates.PointCoordinates;
import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzGeometry;
import com.here.naksha.lib.core.models.geojson.implementation.XyzLineString;
import com.here.naksha.lib.core.models.geojson.implementation.XyzMultiPoint;
import com.here.naksha.lib.core.models.geojson.implementation.XyzPoint;
import com.here.naksha.lib.core.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.naksha.lib.core.models.naksha.NakshaFeature;
import com.here.naksha.lib.core.models.naksha.XyzCollection;
import com.here.naksha.lib.core.models.storage.EExecutedOp;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.SOp;
import com.here.naksha.lib.core.models.storage.WriteXyzCollections;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.Point;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIf;
import org.postgresql.util.PSQLException;

@SuppressWarnings({"unused"})
@TestMethodOrder(OrderAnnotation.class)
public class PsqlStorageTests extends PsqlTests {

  @Override
  boolean enabled() {
    return false;
  }

  final @NotNull String collectionId() {
    return "foo";
  }

  @Override
  boolean partition() {
    return false;
  }

  static final String SINGLE_FEATURE_ID = "TheFeature";

  @Test
  @Order(60)
  @EnabledIf("runTest")
  void singleFeatureCreate() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature feature = new XyzFeature(SINGLE_FEATURE_ID);
    feature.setGeometry(new XyzPoint(5.0d, 6.0d, 2.0d));
    request.add(EWriteOp.CREATE, feature);
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
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
      assertFalse(cursor.hasNext());
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(61)
  @EnabledIf("runTest")
  void singleFeatureRead() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    ReadFeatures readFeatures = new ReadFeatures(collectionId());
    readFeatures.setPropertyOp(POp.eq(PRef.id(), SINGLE_FEATURE_ID));
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(readFeatures).getXyzFeatureCursor()) {
      assertTrue(cursor.hasNext());
      assertTrue(cursor.next());
      final EExecutedOp op = cursor.getOp();
      assertSame(EExecutedOp.READ, op);

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
      assertEquals(storage.getStorageId(), uuidFields[0]);
      assertEquals(collectionId(), uuidFields[1]);
      assertEquals(4, uuidFields[2].length()); // year (4- digits)
      assertEquals(2, uuidFields[3].length()); // hour (2- digits)
      assertEquals(2, uuidFields[4].length()); // minute (2- digits)
      assertEquals("1", uuidFields[5]); // seq id
      final String txnFromUuid = uuidFields[2] + uuidFields[3] + uuidFields[4] + "0000000000" + uuidFields[5];
      assertEquals(txnFromUuid, xyz.getTxn().toString()); // seq id
      assertEquals(TEST_APP_ID, xyz.getAppId());
      assertEquals(TEST_AUTHOR, xyz.getAuthor());
      assertNotEquals(xyz.getRealTimeCreateAt(), xyz.getCreatedAt());
      assertEquals(xyz.getCreatedAt(), xyz.getUpdatedAt());

      assertEquals(encodeLatLon(coordinate.y, coordinate.x, 7), xyz.get("grid"));

      assertFalse(cursor.hasNext());
    }
  }

  @Test
  @Order(62)
  @EnabledIf("runTest")
  void readByBbox() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    Geometry envelopeBbox = bbox(4.0d, 5.0, 5.5d, 6.5);

    ReadFeatures readFeatures = new ReadFeatures(collectionId());
    readFeatures.setPropertyOp(SOp.intersects(envelopeBbox));

    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(readFeatures).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
      // then
      assertEquals(SINGLE_FEATURE_ID, cursor.getFeature().getId());
      assertFalse(cursor.hasNext());
    }
  }

  @Test
  @Order(63)
  @EnabledIf("runTest")
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
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    request.add(EWriteOp.UPDATE, featureToUpdate);
    // when
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
      cursor.next();

      // then
      final XyzFeature feature = cursor.getFeature();
      assertSame(EExecutedOp.UPDATED, cursor.getOp());
      assertEquals(SINGLE_FEATURE_ID, cursor.getId());
      //      assertNotNull(cursor.getPropertiesType());
      final Geometry coordinate = cursor.getGeometry();
      assertEquals(multiPoint.convertToJTSGeometry(), coordinate);
      assertEquals("Bank", feature.get("title").toString());
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(64)
  @EnabledIf("runTest")
  void singleFeatureUpdateVerify() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    // given
    /**
     * data inserted in {@link #singleFeatureCreate()} test and updated by {@link #singleFeatureUpdate()}.
     */
    final ReadFeatures request = RequestHelper.readFeaturesByIdRequest(collectionId(), SINGLE_FEATURE_ID);

    // when
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
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

      assertEquals(storage.getStorageId(), uuidFields[0]);
      assertEquals(collectionId(), uuidFields[1]);
      assertEquals(4, uuidFields[2].length()); // year (4- digits)
      assertEquals(2, uuidFields[3].length()); // hour (2- digits)
      assertEquals(2, uuidFields[4].length()); // minute (2- digits)
      // should have next seq id, which is 2:
      assertEquals("2", uuidFields[5]); // seq id
      final String txnFromUuid = uuidFields[2] + uuidFields[3] + uuidFields[4] + "0000000000" + uuidFields[5];
      assertEquals(txnFromUuid, xyz.getTxn().toString()); // seq id
      assertEquals(TEST_APP_ID, xyz.getAppId());
      assertEquals(TEST_AUTHOR, xyz.getAuthor());

      Point centroid = geometry.getJTSGeometry().getCentroid();
      assertEquals(encodeLatLon(centroid.getY(), centroid.getX(), 7), xyz.get("grid"));
      assertFalse(cursor.hasNext());
    }
  }

  @Test
  @Order(65)
  @EnabledIf("runTest")
  void singleFeatureCreateWitSameId() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature feature = new XyzFeature(SINGLE_FEATURE_ID);
    feature.setGeometry(new XyzPoint(5.0d, 6.0d, 2.0d));
    request.add(EWriteOp.CREATE, feature);
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
      // should change to operation update as row already exists.
      assertSame(EExecutedOp.UPDATED, cursor.getOp());
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(66)
  @EnabledIf("runTest")
  void singleFeatureDelete() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature feature = new XyzFeature(SINGLE_FEATURE_ID);
    feature.setGeometry(new XyzPoint(5.0d, 6.0d, 2.0d));
    request.add(EWriteOp.DELETE, feature);
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
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
      // this geometry differs than requested, because value in db has been changed by update method, so we return
      // what was actually deleted.
      assertEquals(5.0d, coordinate.getOrdinate(0));
      assertEquals(6.0d, coordinate.getOrdinate(1));
      assertEquals(2.0d, coordinate.getOrdinate(2));
      final XyzFeature f = cursor.getFeature();
      assertNotNull(f);
      assertEquals(SINGLE_FEATURE_ID, f.getId());
      assertEquals(uuid, f.xyz().getUuid());
      assertSame(EXyzAction.DELETE, f.xyz().getAction());
      assertFalse(cursor.hasNext());
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(67)
  @EnabledIf("runTest")
  void singleFeatureDeleteVerify() throws SQLException, NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    // when
    /**
     * Read from feature should return nothing.
     */
    final ReadFeatures request = RequestHelper.readFeaturesByIdRequest(collectionId(), SINGLE_FEATURE_ID);
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
      assertFalse(cursor.hasNext());
    }
    // also: direct query to feature table should return nothing.
    try (final PsqlReadSession session = storage.newReadSession(null, true)) {
      ResultSet rs = getFeatureFromTable(session, collectionId(), SINGLE_FEATURE_ID);
      assertFalse(rs.next());
    }

    /**
     * Read from deleted should return valid feature.
     */
    final ReadFeatures requestWithDeleted =
        RequestHelper.readFeaturesByIdRequest(collectionId(), SINGLE_FEATURE_ID);
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
    final String collectionDelTableName = collectionId() + "_del";
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
  @Order(68)
  @EnabledIf("runTest")
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
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    request.add(EWriteOp.PURGE, featureToPurge);

    // when
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
      cursor.next();

      // then
      final XyzFeature feature = cursor.getFeature();
      assertSame(EExecutedOp.PURGED, cursor.getOp());
      assertEquals(SINGLE_FEATURE_ID, cursor.getId());
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(69)
  @EnabledIf("runTest")
  void singleFeaturePurgeVerify() throws SQLException {
    assertNotNull(storage);
    assertNotNull(session);
    // given
    final String collectionDelTableName = collectionId() + "_del";

    // when
    try (final PsqlReadSession session = storage.newReadSession(null, true)) {
      ResultSet rs = getFeatureFromTable(session, collectionDelTableName, SINGLE_FEATURE_ID);

      // then
      assertFalse(rs.next());
    }
  }

  @Test
  @Order(70)
  @EnabledIf("runTest")
  void multipleFeaturesInsert() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    int i = 0;
    boolean firstNameAdded = false;
    while (i < 1000 || !firstNameAdded) {
      final XyzFeature feature = fg.newRandomFeature();
      if (!firstNameAdded) {
        firstNameAdded =
            Objects.equals(fg.firstNames[0], feature.getProperties().get("firstName"));
      }
      request.add(EWriteOp.PUT, feature);
      i++;
    }
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
      for (int j = 0; j < i; j++) {
        assertTrue(cursor.next());
        final EExecutedOp op = cursor.getOp();
        assertSame(EExecutedOp.CREATED, op);
        final String id = cursor.getId();
        assertNotNull(id);
        final String uuid = cursor.getUuid();
        assertNotNull(uuid);
        final Geometry geometry = cursor.getGeometry();
        assertNotNull(geometry);
        final XyzFeature f = cursor.getFeature();
        assertNotNull(f);
        assertEquals(id, f.getId());
        assertEquals(uuid, f.xyz().getUuid());
        assertSame(EXyzAction.CREATE, f.xyz().getAction());
      }
      assertFalse(cursor.hasNext());
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(71)
  @EnabledIf("runTest")
  void multipleFeaturesRead() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final ReadFeatures request = new ReadFeatures(collectionId());
    request.setPropertyOp(POp.or(
        POp.exists(PRef.tag("@:firstName:" + fg.firstNames[0])),
        POp.exists(PRef.tag("@:firstName:" + fg.firstNames[1]))));
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
      // We expect that at least one feature was found!
      assertTrue(cursor.hasNext());
      while (cursor.hasNext()) {
        assertTrue(cursor.next());
        final EExecutedOp op = cursor.getOp();
        assertSame(EExecutedOp.READ, op);
        final String id = cursor.getId();
        assertNotNull(id);
        final String uuid = cursor.getUuid();
        assertNotNull(uuid);
        final Geometry geometry = cursor.getGeometry();
        assertNotNull(geometry);
        final XyzFeature f = cursor.getFeature();
        assertNotNull(f);
        assertEquals(id, f.getId());
        assertEquals(uuid, f.xyz().getUuid());
        assertSame(EXyzAction.CREATE, f.xyz().getAction());
        final List<@NotNull String> tags = f.xyz().getTags();
        assertNotNull(tags);
        assertTrue(tags.size() > 0);
        assertTrue(tags.contains("@:firstName:" + fg.firstNames[0])
            || tags.contains("@:firstName:" + fg.firstNames[1]));
      }
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(110)
  @EnabledIf("runTest")
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
  @Order(111)
  @EnabledIf("runTest")
  void intersectionSearch() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature feature = new XyzFeature("otherFeature");
    LineStringCoordinates lineStringCoordinates = new LineStringCoordinates();
    lineStringCoordinates.add(new PointCoordinates(4.0d, 5.0));
    lineStringCoordinates.add(new PointCoordinates(4.0d, 6.0));

    XyzLineString lineString = new XyzLineString();
    lineString.withCoordinates(lineStringCoordinates);

    feature.setGeometry(lineString);
    request.add(EWriteOp.CREATE, feature);
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
    } finally {
      session.commit(true);
    }

    // read by bbox that surrounds only first point

    Geometry envelopeBbox = bbox(3.9d, 4.9, 4.1d, 5.1);
    ReadFeatures readFeatures = new ReadFeatures(collectionId());
    readFeatures.setPropertyOp(SOp.intersects(envelopeBbox));

    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(readFeatures).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
      // then
      assertEquals("otherFeature", cursor.getFeature().getId());
      assertFalse(cursor.hasNext());
    }
  }

  @Test
  @Order(120)
  @EnabledIf("runTest")
  void dropFooCollection() throws NoCursor, SQLException {
    assertNotNull(storage);
    assertNotNull(session);

    final XyzCollection deleteCollection = new XyzCollection(collectionId());
    final WriteXyzCollections deleteRequest = new WriteXyzCollections();
    deleteRequest.add(EWriteOp.DELETE, deleteCollection);
    try (final ForwardCursor<XyzCollection, XyzCollectionCodec> cursor =
        session.execute(deleteRequest).getXyzCollectionCursor()) {
      assertTrue(cursor.next());
      session.commit(true);

      PsqlReadSession readDeletedSession = storage.newReadSession(nakshaContext, false);
      ResultSet readRs = getFeatureFromTable(readDeletedSession, collectionId(), SINGLE_FEATURE_ID);
      // It should not have any data but table still exists.
      assertFalse(readRs.next());
      readDeletedSession.close();

      // purge
      final WriteXyzCollections purgeRequest = new WriteXyzCollections();
      deleteRequest.add(EWriteOp.PURGE, deleteCollection);
      try (final ForwardCursor<XyzCollection, XyzCollectionCodec> cursorPurge =
          session.execute(deleteRequest).getXyzCollectionCursor()) {
        session.commit(true);
      }

      // try readSession after purge, table doesn't exist anymore, so it should throw an exception.
      PsqlReadSession readPurgedSession = storage.newReadSession(nakshaContext, false);
      assertThrowsExactly(
          PSQLException.class,
          () -> getFeatureFromTable(readPurgedSession, collectionId(), SINGLE_FEATURE_ID),
          "ERROR: relation \"foo\" does not exist");
      readPurgedSession.close();
    }
  }

  private ResultSet getFeatureFromTable(PsqlReadSession session, String table, String featureId) throws SQLException {
    final PostgresSession pgSession = session.session();
    final SQL sql = pgSession.sql().add("SELECT * from ").addIdent(table).add(" WHERE jsondata->>'id' = ? ;");
    final PreparedStatement stmt = pgSession.prepareStatement(sql);
    stmt.setString(1, featureId);
    return stmt.executeQuery();
  }

  private Geometry bbox(Double x1, Double y1, Double x2, Double y2) {
    MultiPointCoordinates multiPoint = new MultiPointCoordinates();
    multiPoint.add(new PointCoordinates(x1, y1));
    multiPoint.add(new PointCoordinates(x2, y2));
    MultiPoint jtsMultiPoint = JTSHelper.toMultiPoint(multiPoint);
    return jtsMultiPoint.getEnvelope();
  }
}
