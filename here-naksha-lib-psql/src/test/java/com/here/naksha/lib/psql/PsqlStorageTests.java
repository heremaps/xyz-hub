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

import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.geojson.implementation.EXyzAction;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.geojson.implementation.XyzPoint;
import com.here.naksha.lib.core.models.storage.*;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.MethodOrderer.OrderAnnotation;
import org.junit.jupiter.api.condition.EnabledIf;

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
    }
    // TODO: Read the created feature and review that it is in version 1, and everything else is good.
    //       Optionally, ensure that the "grid" is the correct Geo-Hash!
  }

  @Test
  @Order(62)
  @EnabledIf("runTest")
  void singleFeatureUpdate() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    // TODO: Update the feature
  }

  @Test
  @Order(63)
  @EnabledIf("runTest")
  void singleFeatureUpdateVerify() throws NoCursor {
    assertNotNull(storage);
    assertNotNull(session);
    // TODO: Read the updated feature and review that it is in version 2, and everything else is good.
    //       Optionally, ensure that the "grid" is the correct Geo-Hash!
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
  void singleFeatureDeleteVerify() {
    assertNotNull(storage);
    assertNotNull(session);
    // TODO: Ensure that the feature is deleted (not found)
    //       Ensure that the feature is available when reading including delete features, verify state (version 3)
    // aso.
    //       Directly query database in "del" table and review that the feature exists there in the correct staet.
  }

  @Test
  @Order(66)
  @EnabledIf("runTest")
  void singleFeaturePurge() {
    assertNotNull(storage);
    assertNotNull(session);
    // TODO: Purge the deleted feature.
    //       Ensure that the feature is no longer available in "_del" table
  }

  @Test
  @Order(67)
  @EnabledIf("runTest")
  void singleFeaturePurgeVerify() {
    assertNotNull(storage);
    assertNotNull(session);
    // TODO: Ensure that the feature is no longer available in "_del" table.
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
  @Order(120)
  @EnabledIf("runTest")
  void dropFooCollection() {
    // TODO: First try to DELETE the test collection
    //       Then try to PURGE the test collection
    //       Finally try to read the test collection (it should not exist)
    assertNotNull(storage);
    assertNotNull(session);
    //    StorageCollection storageCollection = new StorageCollection(COLLECTION_ID);
    //    WriteOp<StorageCollection> writeOp = new WriteOp<>(EWriteOp.DELETE, storageCollection, false);
    //    WriteCollections<StorageCollection> writeRequest = new WriteCollections<>(List.of(writeOp));
    //    final WriteResult<StorageCollection> dropResult =
    //        (WriteResult<StorageCollection>) session.execute(writeRequest);
    //    session.commit();
    //    assertNotNull(dropResult);
    //    StorageCollection dropped = dropResult.results.get(0).feature;
    //    if (dropFinally()) {
    //      assertNotSame(storageCollection, dropped);
    //    } else {
    //      assertNotSame(storageCollection, dropped);
    //    }
    //    assertEquals(storageCollection.getId(), dropped.getId());
    //    //    assertEquals(storageCollection.getHistory(), dropped.getHistory());
    //    assertEquals(storageCollection.getMaxAge(), dropped.getMaxAge());
    //    ReadCollections readRequest =
    //        new ReadCollections().withIds(COLLECTION_ID).withReadDeleted(false);
    //    XyzFeatureReadResult result = (XyzFeatureReadResult) session.execute(readRequest);
    //    if (dropFinally()) {
    //      assertFalse(result.hasNext());
    //    } else {
    //      assertTrue(result.hasNext());
    //    }
  }
}
