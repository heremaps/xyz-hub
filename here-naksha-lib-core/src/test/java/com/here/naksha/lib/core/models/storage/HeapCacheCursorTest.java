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
package com.here.naksha.lib.core.models.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.Test;

public class HeapCacheCursorTest {

  @Test
  void testAlreadyUsedFrowardCursorConversion() throws NoCursor {
    // given
    long limit = 10;
    SuccessResult result = limitedResult(limit);
    ForwardCursor<XyzFeature, XyzFeatureCodec> cursor = result.getXyzFeatureCursor();

    // when
    cursor.next();
    SeekableCursor<XyzFeature, XyzFeatureCodec> seekableCursor = result.getXyzSeekableCursor();

    // then
    assertEquals(limit - 1, cursor.position);
    // check if seekable cursor position change doesn't affect original cursor position change.
    seekableCursor.afterLast();
    assertEquals(limit - 1, cursor.position);
    assertNotNull(cursor.getId());
    assertNotNull(cursor.getUuid());
    assertNull(cursor.getError());
    assertFalse(cursor.hasError());
    assertSame(EExecutedOp.CREATED, cursor.getOp());
  }

  @Test
  void testLimitCacheElements() throws NoCursor {
    // given
    long limit = 100;
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        limitedResult(limit).getXyzSeekableCursor();

    // when
    long count = 0;
    for (XyzFeature row : cursor) {
      assertNotNull(row.getId());
      count++;
    }

    // then
    assertEquals(limit, count);
    assertFalse(cursor.hasNext());
  }

  @Test
  void testMoveCursorBeforeFirst() throws NoCursor {
    // given
    long limit = 50;
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        limitedResult(limit).getXyzSeekableCursor();

    // when
    cursor.next();
    XyzFeature firstFeature = cursor.getFeature();
    String firstId = cursor.getId();
    cursor.next();
    XyzFeature secondFeature = cursor.getFeature();
    cursor.beforeFirst();
    cursor.next();
    XyzFeature featureAfterBackToTop = cursor.getFeature();
    String featureAfterBackToTopId = cursor.getId();

    // then
    assertEquals(firstFeature, featureAfterBackToTop);
    assertEquals(firstId, featureAfterBackToTopId);
    assertNotEquals(firstFeature, secondFeature);
    assertEquals(0, cursor.position);
  }

  @Test
  void testBackToFirstElement() throws NoCursor {
    long limit = 5;
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        limitedResult(limit).getXyzSeekableCursor();

    // when
    cursor.next();
    XyzFeature firstFeature = cursor.getFeature();
    cursor.next();
    XyzFeature secondFeature = cursor.getFeature();
    cursor.first();
    XyzFeature rewindedFirst = cursor.getFeature();

    // then
    assertEquals(firstFeature, rewindedFirst);
    assertNotEquals(firstFeature, secondFeature);
    assertEquals(0, cursor.position);
  }

  @Test
  void testGoToLast() throws NoCursor {
    long limit = 5;
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        limitedResult(limit).getXyzSeekableCursor();

    // when
    cursor.last();
    XyzFeature lastFeature = cursor.getFeature();
    cursor.beforeFirst();

    XyzFeature lastByIterator = null;
    while (cursor.hasNext() && cursor.next()) {
      lastByIterator = cursor.getFeature();
    }

    // then
    assertEquals(lastFeature, lastByIterator);
    assertEquals(4, cursor.position);
  }

  @Test
  void testAfterLast() throws NoCursor {
    // given
    long limit = 5;
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        limitedResult(limit).getXyzSeekableCursor();

    // when
    cursor.afterLast();

    // then
    assertFalse(cursor.hasNext());
    assertEquals(limit, cursor.position);
  }

  @Test
  void testRelativeMove() throws NoCursor {
    // given
    long limit = 5;
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        limitedResult(limit).getXyzSeekableCursor();

    // expect
    assertEquals(-1, cursor.position);

    assertTrue(cursor.next());
    assertEquals(0, cursor.position);

    assertTrue(cursor.relative(2));
    assertEquals(2, cursor.position);

    assertTrue(cursor.relative(-1));
    assertEquals(1, cursor.position);

    assertFalse(cursor.relative(-5));
    assertEquals(-1, cursor.position);

    assertFalse(cursor.relative(10));
    assertEquals(limit, cursor.position);
  }

  @Test
  void testAbsoluteMove() throws NoCursor {
    // given
    long limit = 5;
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        limitedResult(limit).getXyzSeekableCursor();

    // expect
    assertEquals(-1, cursor.position);

    assertTrue(cursor.next());
    assertEquals(0, cursor.position);

    assertTrue(cursor.absolute(2));
    assertEquals(2, cursor.position);

    assertTrue(cursor.absolute(1));
    assertEquals(1, cursor.position);

    assertFalse(cursor.absolute(-5));
    assertEquals(-1, cursor.position);

    assertFalse(cursor.absolute(10));
    assertEquals(limit, cursor.position);
  }

  @Test
  void testPrevious() throws NoCursor {
    // given
    long limit = 5;
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        limitedResult(limit).getXyzSeekableCursor();

    // expect
    assertTrue(cursor.next());
    assertTrue(cursor.next());
    assertEquals(1, cursor.position);

    assertTrue(cursor.previous());
    assertEquals(0, cursor.position);

    assertFalse(cursor.previous());
    assertEquals(-1, cursor.position);
  }

  @Test
  void testLimit0() throws NoCursor {
    // given
    long limit = 0;
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor =
        limitedResult(limit).getXyzSeekableCursor();

    // expect
    assertFalse(cursor.hasNext());
  }

  @Test
  void testEmptyRs() throws NoCursor {
    // given
    EmptyForwardCursor<XyzFeature, XyzFeatureCodec> emptyForwardCursor =
        new EmptyForwardCursor<>(XyzFeatureCodecFactory.get());

    MockResult<XyzFeature> result = new MockResult<>(emptyForwardCursor);

    // when
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor = result.getXyzSeekableCursor();

    // then
    assertFalse(cursor.hasNext());
  }

  @Test
  void testCacheAllAvailableRows() throws NoCursor {
    // given
    long rsSize = 10;
    LimitedForwardCursor<XyzFeature, XyzFeatureCodec> limitedForwardCursor =
        new LimitedForwardCursor<>(XyzFeatureCodecFactory.get(), rsSize);
    MockResult<XyzFeature> result = new MockResult<>(limitedForwardCursor);

    // when
    SeekableCursor<XyzFeature, XyzFeatureCodec> cursor = result.getXyzSeekableCursor();
    cursor.afterLast();

    // then
    assertEquals(rsSize, cursor.position);
  }

  @Test
  void testAddFeature() throws NoCursor {
    // given
    long limit = 5;
    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = limitedResult(limit).mutableCursor();

    XyzFeature newFeature = new XyzFeature("new_feature_1");

    // when
    cursor.last();
    XyzFeature lastFeatureBeforeAdd = cursor.getFeature();
    cursor.addFeature(newFeature);
    cursor.last();
    XyzFeature lastFeatureAfterAdd = cursor.getFeature();

    // then
    assertNotEquals(newFeature, lastFeatureBeforeAdd);
    assertEquals(newFeature, lastFeatureAfterAdd);
  }

  @Test
  void testSetFeatureAtPosition() throws NoCursor {
    // given
    long limit = 5;
    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = limitedResult(limit).mutableCursor();

    XyzFeature newFeature = new XyzFeature("new_feature_1");

    // when
    cursor.first();
    XyzFeature firstFeatureBeforeReplace = cursor.getFeature();
    XyzFeature replacedFeature = cursor.setFeature(0, newFeature);
    cursor.last();
    cursor.first();
    XyzFeature firstFeatureAfterReplace = cursor.getFeature();

    // then
    assertNotEquals(newFeature, firstFeatureBeforeReplace);
    assertEquals(replacedFeature, firstFeatureBeforeReplace);
    assertEquals(newFeature, firstFeatureAfterReplace);
    // size of cursor has not changed
    cursor.afterLast();
    assertEquals(limit, cursor.position);
  }

  @Test
  void testSetFeature() throws NoCursor {
    // given
    long limit = 5;
    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = limitedResult(limit).mutableCursor();

    XyzFeature newFeature = new XyzFeature("new_feature_1");

    // when
    cursor.last();
    XyzFeature firstFeatureBeforeReplace = cursor.getFeature();
    XyzFeature replacedFeature = cursor.setFeature(newFeature);
    XyzFeature firstFeatureAfterReplace = cursor.getFeature();

    // then
    assertNotEquals(newFeature, firstFeatureBeforeReplace);
    assertEquals(replacedFeature, firstFeatureBeforeReplace);
    assertEquals(newFeature, firstFeatureAfterReplace);
    // size of cursor has not changed
    cursor.afterLast();
    assertEquals(limit, cursor.position);
  }

  @Test
  void testRemoveFeatureAtPosition() throws NoCursor {
    // given
    long limit = 5;
    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = limitedResult(limit).mutableCursor();

    // when
    cursor.first();
    XyzFeature firstFeatureBeforeRemove = cursor.getFeature();
    cursor.next();
    XyzFeature secondFeatureBeforeRemove = cursor.getFeature();
    XyzFeature removedFeature = cursor.removeFeature(0);
    cursor.last();
    cursor.first();
    XyzFeature firstFeatureAfterRemove = cursor.getFeature();

    // then
    assertEquals(removedFeature, firstFeatureBeforeRemove);
    assertEquals(secondFeatureBeforeRemove, firstFeatureAfterRemove);
    assertEquals(secondFeatureBeforeRemove, firstFeatureAfterRemove);
    // size of cursor has changed
    cursor.afterLast();
    assertEquals(limit - 1, cursor.position);
  }

  @Test
  void testRemoveFeatureAtCurrentPosition() throws NoCursor {
    // given
    long limit = 3;
    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = limitedResult(limit).mutableCursor();

    // when
    cursor.first();
    XyzFeature firstFeatureBeforeRemove = cursor.getFeature();
    cursor.last();
    cursor.removeFeature();
    cursor.last();
    cursor.removeFeature();
    cursor.last();
    XyzFeature lastFeatureAfterRemovals = cursor.getFeature();

    // then
    assertEquals(lastFeatureAfterRemovals, firstFeatureBeforeRemove);
    // size of cursor has changed
    cursor.afterLast();
    assertEquals(1, cursor.position);
  }

  @Test
  void shouldThrowExceptionWhenAskingForPositionOutOfRange() throws NoCursor {
    // given
    long limit = 3;
    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = limitedResult(limit).mutableCursor();
    XyzFeature newFeature = new XyzFeature("new_feature_1");

    // expect
    assertThrows(NoSuchElementException.class, () -> cursor.removeFeature(100));
    assertThrows(NoSuchElementException.class, () -> cursor.removeFeature(-1));
    assertThrows(NoSuchElementException.class, () -> cursor.setFeature(-100, newFeature));
    assertThrows(NoSuchElementException.class, () -> cursor.setFeature(-1, newFeature));
  }

  @Test
  void emptyJsonShouldGiveNullFeature() throws NoCursor {
    // given
    String json = null;
    LimitedForwardCursor<XyzFeature, XyzFeatureCodec> infiniteForwardCursor =
        new LimitedForwardCursor<>(XyzFeatureCodecFactory.get(), 1, json);
    MockResult<XyzFeature> result = new MockResult<>(infiniteForwardCursor);

    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = result.mutableCursor();

    // when
    cursor.first();

    // then
    assertNotNull(cursor.getId());
    assertNull(cursor.getFeature());
  }

  @Test
  void codecChange() throws NoCursor {
    // given
    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = limitedResult(1).mutableCursor();

    // when
    cursor.next();
    cursor.getFeature();
    MutableCursor<String, StringCodec> cursorString = cursor.withCodecFactory(new StringCodecFactory(), false);
    cursorString.first();
    String feature = cursorString.getFeature();

    // then
    assertEquals("{\"type\":\"Feature\"}", feature);
    assertNotNull(cursorString.getId());
    assertNotNull(cursorString.getUuid());
    assertNull(cursorString.getError());
    assertFalse(cursorString.hasError());
    assertSame(EExecutedOp.CREATED, cursorString.getOp());
  }

  @Test
  void getFeatureBeforeFirst() throws NoCursor {
    // given
    MutableCursor<XyzFeature, XyzFeatureCodec> cursor = limitedResult(1).mutableCursor();

    // expect
    cursor.next();
    cursor.beforeFirst();

    assertThrows(NoSuchElementException.class, cursor::getFeature);
  }

  private SuccessResult limitedResult(long limit) {
    return new MockResult<>(new LimitedForwardCursor<>(XyzFeatureCodecFactory.get(), limit));
  }
}
