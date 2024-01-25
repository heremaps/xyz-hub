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

import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;
import static com.here.naksha.lib.core.models.storage.POp.and;
import static com.here.naksha.lib.core.models.storage.POp.eq;
import static com.here.naksha.lib.core.models.storage.POp.exists;
import static com.here.naksha.lib.core.models.storage.POp.not;
import static com.here.naksha.lib.core.models.storage.PRef.id;
import static com.here.naksha.lib.core.util.storage.RequestHelper.createBBoxEnvelope;
import static com.spatial4j.core.io.GeohashUtils.encodeLatLon;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.XyzError;
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
import com.here.naksha.lib.core.models.storage.CodecError;
import com.here.naksha.lib.core.models.storage.EExecutedOp;
import com.here.naksha.lib.core.models.storage.EWriteOp;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.here.naksha.lib.core.models.storage.MutableCursor;
import com.here.naksha.lib.core.models.storage.NonIndexedPRef;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.PRef;
import com.here.naksha.lib.core.models.storage.ReadFeatures;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SOp;
import com.here.naksha.lib.core.models.storage.SeekableCursor;
import com.here.naksha.lib.core.models.storage.WriteFeatures;
import com.here.naksha.lib.core.models.storage.WriteXyzCollections;
import com.here.naksha.lib.core.models.storage.WriteXyzFeatures;
import com.here.naksha.lib.core.models.storage.XyzCollectionCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.util.json.Json;
import com.here.naksha.lib.core.util.json.JsonMap;
import com.here.naksha.lib.core.util.json.JsonObject;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.operation.buffer.BufferOp;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

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
    return true;
  }

  final @NotNull String collectionId() {
    return "foo";
  }

  @Override
  boolean partition() {
    return false;
  }

  static final String SINGLE_FEATURE_ID = "TheFeature";
  static final String SINGLE_FEATURE_INITIAL_TAG = "@:foo:world";
  static final String SINGLE_FEATURE_REPLACEMENT_TAG = "@:foo:bar";

  @Test
  @Order(50)
  @EnabledIf("runTest")
  void singleFeatureCreate() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature feature = new XyzFeature(SINGLE_FEATURE_ID);
    feature.setGeometry(new XyzPoint(5.0d, 6.0d, 2.0d));
    feature.xyz().addTag(SINGLE_FEATURE_INITIAL_TAG, false);
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
      assertEquals(List.of(SINGLE_FEATURE_INITIAL_TAG), f.xyz().getTags());
      assertFalse(cursor.hasNext());
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(51)
  @EnabledIf("runTest")
  void singleFeatureRead() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    ReadFeatures readFeatures = new ReadFeatures(collectionId());
    readFeatures.setPropertyOp(eq(id(), SINGLE_FEATURE_ID));
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
      assertEquals(cursor.getUuid(), xyz.getUuid());
      final String[] uuidFields = uuid.split(":");
      assertEquals(storage.getStorageId(), uuidFields[0]);
      assertEquals(collectionId(), uuidFields[1]);
      assertEquals(4, uuidFields[2].length()); // year (4- digits)
      assertEquals(2, uuidFields[3].length()); // hour (2- digits)
      assertEquals(2, uuidFields[4].length()); // minute (2- digits)
      assertEquals("1", uuidFields[5]); // seq id
      assertEquals(TEST_APP_ID, xyz.getAppId());
      assertEquals(TEST_AUTHOR, xyz.getAuthor());
      assertNotEquals(xyz.getRealTimeUpdatedAt(), xyz.getUpdatedAt());
      assertEquals(xyz.getCreatedAt(), xyz.getUpdatedAt());

      assertEquals(encodeLatLon(coordinate.y, coordinate.x, 7), xyz.get("grid"));

      assertEquals(List.of(SINGLE_FEATURE_INITIAL_TAG), xyz.getTags());

      assertFalse(cursor.hasNext());
    }
  }

  @Test
  @Order(52)
  @EnabledIf("runTest")
  void readByBbox() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    Geometry envelopeBbox = createBBoxEnvelope(4.0d, 5.0, 5.5d, 6.5);

    ReadFeatures readFeatures = new ReadFeatures(collectionId());
    BufferOp.bufferOp(envelopeBbox, 1.0);
    readFeatures.setSpatialOp(SOp.intersects(envelopeBbox));

    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(readFeatures).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
      // then
      assertEquals(SINGLE_FEATURE_ID, cursor.getFeature().getId());
      assertFalse(cursor.hasNext());
    }
  }

  @Test
  @Order(52)
  @EnabledIf("runTest")
  void readyWithBuffer() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    XyzPoint xyzPoint = new XyzPoint(4.0d, 5.0d);

    ReadFeatures readFeatures = new ReadFeatures(collectionId());
    readFeatures.setSpatialOp(SOp.intersectsWithBuffer(xyzPoint, 1.0));

    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(readFeatures).getXyzFeatureCursor()) {
      assertFalse(cursor.hasNext());
    }

    readFeatures.setSpatialOp(SOp.intersectsWithBuffer(xyzPoint, 2.0));
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(readFeatures).getXyzFeatureCursor()) {
      assertTrue(cursor.hasNext());
    }
  }

  @Test
  @Order(54)
  @EnabledIf("runTest")
  void singleFeatureUpsert() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    // given
    final NakshaFeature featureToUpdate = new NakshaFeature(SINGLE_FEATURE_ID);
    final XyzPoint xyzGeometry = new XyzPoint(5.0d, 6.0d, 2.0d);
    featureToUpdate.setGeometry(xyzGeometry);
    featureToUpdate.xyz().addTag(SINGLE_FEATURE_INITIAL_TAG, false);
    featureToUpdate.xyz().addTag(SINGLE_FEATURE_REPLACEMENT_TAG, false);

    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    request.add(EWriteOp.PUT, featureToUpdate);
    // when
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
      cursor.next();

      // then
      final XyzFeature feature = cursor.getFeature();
      assertSame(EExecutedOp.UPDATED, cursor.getOp());
      assertEquals(SINGLE_FEATURE_ID, cursor.getId());
      final Geometry coordinate = cursor.getGeometry();
      assertEquals(xyzGeometry.convertToJTSGeometry(), coordinate);
      assertEquals(
          asList(SINGLE_FEATURE_INITIAL_TAG, SINGLE_FEATURE_REPLACEMENT_TAG),
          feature.xyz().getTags());
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(55)
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
    // This tag should replace the previous one!
    featureToUpdate.xyz().addTag(SINGLE_FEATURE_REPLACEMENT_TAG, false);
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
      final String title = assertInstanceOf(String.class, feature.get("title"));
      assertEquals("Bank", title);
      assertEquals(List.of(SINGLE_FEATURE_REPLACEMENT_TAG), feature.xyz().getTags());
    } finally {
      session.commit(true);
    }
  }

  private static final int GUID_STORAGE_ID = 0;
  private static final int GUID_COLLECTION_ID = 1;
  private static final int GUID_YEAR = 2;
  private static final int GUID_MONTH = 3;
  private static final int GUID_DAY = 4;
  private static final int GUID_ID = 5;

  @Test
  @Order(56)
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
      assertEquals(3, xyz.getVersion());
      assertSame(EXyzAction.UPDATE, xyz.getAction());
      final XyzGeometry geometry = feature.getGeometry();
      assertNotNull(geometry);
      Coordinate expectedGeometry = new Coordinate(5.15d, 6.0d, 2.15d);
      assertEquals(expectedGeometry, geometry.getJTSGeometry().getCoordinate());

      final String uuid = cursor.getUuid();
      assertEquals(cursor.getUuid(), xyz.getUuid());
      final String[] uuidFields = uuid.split(":");

      assertEquals(storage.getStorageId(), uuidFields[GUID_STORAGE_ID]);
      assertEquals(collectionId(), uuidFields[GUID_COLLECTION_ID]);
      assertEquals(4, uuidFields[GUID_YEAR].length()); // year (4- digits)
      assertEquals(2, uuidFields[GUID_MONTH].length()); // hour (2- digits)
      assertEquals(2, uuidFields[GUID_DAY].length()); // minute (2- digits)
      // Note: We know that the "id" is actually the sequence number of the storage (so "i").
      // - We created a feature (0)
      // - We updated via upsert (2), this created a history entry (1)
      // - Eventually we did an update (4), which again created a history entry (3)
      assertEquals("4", uuidFields[GUID_ID]);
      // Note: We know that if the schema was dropped, the transaction number is reset to 0.
      // - Create the collection in parent PsqlTest (0) <- commit
      // - Create the single feature (1) <- commit
      // - Upsert the single feature (2) <- commit
      // - Update the single feature (3) <- commit
      if (dropInitially()) {
        final long txnFromUuid = Long.parseLong(
            uuidFields[GUID_YEAR] + uuidFields[GUID_MONTH] + uuidFields[GUID_DAY] + "00000000003");
        assertEquals(txnFromUuid, xyz.getTxn()); // seq id
      }
      assertEquals(TEST_APP_ID, xyz.getAppId());
      assertEquals(TEST_AUTHOR, xyz.getAuthor());

      Point centroid = geometry.getJTSGeometry().getCentroid();
      assertEquals(encodeLatLon(centroid.getY(), centroid.getX(), 7), xyz.get("grid"));
      assertFalse(cursor.hasNext());
    }
  }

  @Test
  @Order(57)
  @EnabledIf("runTest")
  void singleFeaturePutWithSameId() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature feature = new XyzFeature(SINGLE_FEATURE_ID);
    feature.setGeometry(new XyzPoint(5.0d, 6.0d, 2.0d));
    request.add(EWriteOp.PUT, feature);
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
  @Order(60)
  @EnabledIf("runTest")
  void testDuplicateFeatureId() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    // given
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature feature = new XyzFeature(SINGLE_FEATURE_ID);
    feature.setGeometry(new XyzPoint(0.0d, 0.0d, 0.0d));
    request.add(EWriteOp.CREATE, feature);

    // when
    final Result result = session.execute(request);

    // then
    assertInstanceOf(ErrorResult.class, result);
    ErrorResult errorResult = (ErrorResult) result;
    assertEquals(XyzError.CONFLICT, errorResult.reason);
    assertEquals("The feature with the id 'TheFeature' does exist already", errorResult.message);
    session.commit(true);

    // make sure feature hasn't been updated (has old geometry).
    final ReadFeatures readRequest = RequestHelper.readFeaturesByIdRequest(collectionId(), SINGLE_FEATURE_ID);
    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
      assertEquals(
          new Coordinate(5d, 6d, 2d),
          cursor.getFeature().getGeometry().getJTSGeometry().getCoordinate());
    }
  }

  @Test
  @Order(61)
  @EnabledIf("runTest")
  void testMultiOperationPartialFail() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    final String UNIQUE_VIOLATION_MSG =
        format("The feature with the id '%s' does exist already", SINGLE_FEATURE_ID);

    // given
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature featureToSucceed = new XyzFeature("123");
    request.add(EWriteOp.CREATE, featureToSucceed);
    final XyzFeature featureToFail = new XyzFeature(SINGLE_FEATURE_ID);
    request.add(EWriteOp.CREATE, featureToFail);

    // when
    final Result result = session.execute(request);

    // then
    assertInstanceOf(ErrorResult.class, result);
    ErrorResult errorResult = (ErrorResult) result;
    assertEquals(XyzError.CONFLICT, errorResult.reason);
    assertEquals(UNIQUE_VIOLATION_MSG, errorResult.message);
    int expectedResults = 2;
    try (ForwardCursor<XyzFeature, XyzFeatureCodec> cursor = result.getXyzFeatureCursor()) {
      while (expectedResults-- > 0) {
        assertTrue(cursor.next());
        final String id = cursor.getFeature().getId();
        if (featureToSucceed.getId().equals(id)) {
          // OK
        } else if (featureToFail.getId().equals(id)) {
          assertTrue(cursor.hasError());
          assertSame(EExecutedOp.ERROR, cursor.getOp());
          CodecError rowError = cursor.getError();
          assertNotNull(rowError);
          assertEquals(XyzError.CONFLICT, rowError.err);
          assertEquals(UNIQUE_VIOLATION_MSG, rowError.msg);
          // we still should be able to read the feature
          assertEquals(featureToFail.getId(), cursor.getFeature().getId());
        } else {
          fail("Received invalid feature id: " + id);
        }
      }
    } finally {
      session.commit(true);
    }
  }

  @Test
  @Order(62)
  @EnabledIf("runTest")
  void testInvalidUuid() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    // given
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature feature = new XyzFeature(SINGLE_FEATURE_ID);
    feature.xyz().setUuid("invalid_UUID");
    request.add(EWriteOp.UPDATE, feature);

    // when
    final Result result = session.execute(request);

    // then
    assertInstanceOf(ErrorResult.class, result);
    ErrorResult errorResult = (ErrorResult) result;
    assertEquals(XyzError.CONFLICT, errorResult.reason);
    assertTrue(
        errorResult.message.startsWith("The feature 'TheFeature' uuid 'invalid_UUID' does not match"),
        errorResult.message);
    session.commit(true);
  }

  @Test
  @Order(64)
  @EnabledIf("runTest")
  void singleFeatureDeleteById() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature feature = new XyzFeature("TO_DEL_BY_ID");
    request.add(EWriteOp.CREATE, feature);

    // when
    try(final Result result = session.execute(request)) {
      final WriteXyzFeatures delRequest = new WriteXyzFeatures(collectionId());
      delRequest.delete("TO_DEL_BY_ID", null);
      try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
               session.execute(delRequest).getXyzFeatureCursor()) {
        assertTrue(cursor.next());
        assertSame(EExecutedOp.DELETED, cursor.getOp());
        assertEquals("TO_DEL_BY_ID", cursor.getId());
        assertFalse(cursor.hasNext());
      } finally {
        session.commit(true);
      }
    }
  }

  @Test
  @Order(64)
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
  @Order(65)
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
  @Order(66)
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
  @Order(67)
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
        exists(PRef.tag("@:firstName:" + fg.firstNames[0])),
        exists(PRef.tag("@:firstName:" + fg.firstNames[1]))));
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
  @Order(72)
  @EnabledIf("runTest")
  void seekableCursorRead() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    final ReadFeatures request = new ReadFeatures(collectionId());
    request.limit = null;
    try (final SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(request).getXyzSeekableCursor()) {

      // commit closes original cursor, but as we have all rows cached SeekableCursor should work as normal.
      session.commit(true);

      // We expect that at least one feature was found!
      assertTrue(cursor.hasNext());
      cursor.next();
      XyzFeature firstFeature = cursor.getFeature();
      while (cursor.hasNext()) {
        assertTrue(cursor.next());
        final XyzFeature f = cursor.getFeature();
        assertNotNull(f);
      }
      assertFalse(cursor.hasNext());

      cursor.beforeFirst();
      assertTrue(cursor.next());
      assertEquals(firstFeature, cursor.getFeature());
    }
  }

  @Test
  @Order(73)
  @EnabledIf("runTest")
  void testRestoreOrder() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);

    // given
    final WriteXyzFeatures request = new WriteXyzFeatures(collectionId());
    final XyzFeature featureToSucceed = new XyzFeature("121");
    request.add(EWriteOp.CREATE, featureToSucceed);
    final XyzFeature featureToFail = new XyzFeature("120");
    request.add(EWriteOp.CREATE, featureToFail);

    // when
    final Result result = session.execute(request);

    // then
    try (MutableCursor<XyzFeature, XyzFeatureCodec> cursor = result.getXyzMutableCursor()) {
      cursor.next();
      assertEquals("120", cursor.getId());
      cursor.next();
      assertEquals("121", cursor.getId());

      assertTrue(cursor.restoreInputOrder());
      cursor.first();
      assertEquals("121", cursor.getId());
      assertEquals(request.features.get(0).getId(), cursor.getId());
      cursor.next();
      assertEquals("120", cursor.getId());
      assertEquals(request.features.get(1).getId(), cursor.getId());
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

    Geometry envelopeBbox = createBBoxEnvelope(3.9d, 4.9, 4.1d, 5.1);
    ReadFeatures readFeatures = new ReadFeatures(collectionId());
    readFeatures.setSpatialOp(SOp.intersects(envelopeBbox));

    try (final ForwardCursor<XyzFeature, XyzFeatureCodec> cursor =
        session.execute(readFeatures).getXyzFeatureCursor()) {
      assertTrue(cursor.next());
      // then
      assertEquals("otherFeature", cursor.getFeature().getId());
      assertFalse(cursor.hasNext());
    }
  }


  @Test
  @Order(112)
  @EnabledIf("runTest")
  void notIndexedPropertyRead() throws NoCursor, IOException {
    assertNotNull(storage);
    assertNotNull(session);

    WriteFeatures<String, StringCodec, ?> request = new WriteFeatures<>(new StringCodecFactory(), collectionId());

    // given
    final String jsonReference = "{\"id\":\"32167\",\"properties\":{\"weight\":60,\"length\":null,\"color\":\"red\",\"ids\":[0,1,9],\"subJson\":{\"b\":1},\"references\":[{\"id\":\"urn:here::here:Topology:106003684\",\"type\":\"Topology\",\"prop\":{\"a\":1}}]}}";
    ObjectReader reader = Json.get().reader();
    request.add(EWriteOp.CREATE, jsonReference);
    try (final MutableCursor<String, StringCodec> cursor =
        session.execute(request).mutableCursor(new StringCodecFactory())) {
      assertTrue(cursor.next());
    } finally {
      session.commit(true);
    }

    Consumer<ReadFeatures> expect = readFeaturesReq -> {
      try (final MutableCursor<String, StringCodec> cursor =
               session.execute(readFeaturesReq).mutableCursor(new StringCodecFactory())) {
        cursor.next();
        assertEquals("32167", cursor.getId());
        assertFalse(cursor.hasNext());
      } catch (NoCursor e) {
        throw unchecked(e);
      }
    };


    // when - search for int value
    ReadFeatures readFeatures = new ReadFeatures(collectionId());
    POp weightSearch = eq(new NonIndexedPRef("properties", "weight"), 60);
    readFeatures.setPropertyOp(weightSearch);
    // then
    expect.accept(readFeatures);

    // when - search 'not'
    readFeatures.setPropertyOp(not(eq(new NonIndexedPRef("properties", "weight"), 59)));
    // then
    expect.accept(readFeatures);

    // when - search 'exists'
    readFeatures.setPropertyOp(exists(new NonIndexedPRef("properties", "weight")));
    // then
    expect.accept(readFeatures);

    // when - search 'not exists'
    readFeatures.setPropertyOp(and(not(exists(new NonIndexedPRef("properties", "weight2"))), eq(id(), "32167")));
    // then
    expect.accept(readFeatures);

    // when - search not null value
    POp exSearch = POp.isNotNull(new NonIndexedPRef("properties", "color"));
    readFeatures.setPropertyOp(exSearch);
    // then
    expect.accept(readFeatures);

    // when - search null value
    POp nullSearch = POp.isNull(new NonIndexedPRef("properties", "length"));
    readFeatures.setPropertyOp(nullSearch);
    // then
    expect.accept(readFeatures);

    // when - search array contains
    POp arraySearch = POp.contains(new NonIndexedPRef("properties", "ids"), 9);
    readFeatures.setPropertyOp(arraySearch);
    // then
    expect.accept(readFeatures);

    // when - search by json object
    POp jsonSearch2 = POp.contains(new NonIndexedPRef("properties", "references"), "[{\"id\":\"urn:here::here:Topology:106003684\"}]");
    readFeatures.setPropertyOp(jsonSearch2);
    // then
    expect.accept(readFeatures);

    // when - search by json object
    POp jsonSearch3 = POp.contains(new NonIndexedPRef("properties", "references"), reader.readValue("[{\"prop\":{\"a\":1}}]", JsonNode.class));
    readFeatures.setPropertyOp(jsonSearch3);
    // then
    expect.accept(readFeatures);

    // when - search by json object
    POp jsonSearch4 = POp.contains(new NonIndexedPRef("properties", "subJson"), reader.readValue("{\"b\":1}", JsonNode.class));
    readFeatures.setPropertyOp(jsonSearch3);
    // then
    expect.accept(readFeatures);
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
}
