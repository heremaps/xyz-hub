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
package com.here.naksha.lib.hub.mock;

import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.storage.EExecutedOp;
import com.here.naksha.lib.core.models.storage.ResultCursor;
import com.vividsolutions.jts.geom.Geometry;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class MockResultCursor<T extends XyzFeature> extends ResultCursor<T> {

  private final @NotNull Class<T> featureType;
  private final @NotNull List<Object> items;
  protected int currentPos;

  MockResultCursor(@NotNull Class<T> featureType, @NotNull List<Object> items) {
    this.featureType = featureType;
    this.items = items;
    currentPos = -1;
  }

  /**
   * Tests whether there is another result.
   *
   * @return {@code true} if another result is available; {@code false} otherwise.
   */
  @Override
  public boolean hasNext() {
    return currentPos < items.size() - 1;
  }

  /**
   * Moves the cursor to the next result.
   *
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  @Override
  public boolean next() {
    currentPos++;
    return isPositionValid();
  }

  /**
   * Moves the cursor to the previous result.
   *
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  @Override
  public boolean previous() {
    currentPos--;
    return isPositionValid();
  }

  /**
   * Moves the cursor before the first result, so that calling {@link #next()} will load the first result.
   */
  @Override
  public void beforeFirst() {
    currentPos = -1;
  }

  /**
   * Moves the cursor to the first result.
   *
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  @Override
  public boolean first() {
    currentPos = 0;
    return isPositionValid();
  }

  /**
   * Moves the cursor behind the last result, so that calling {@link #previous()} will load the last result.
   */
  @Override
  public void afterLast() {
    currentPos = items.size();
  }

  /**
   * Moves the cursor to the last result.
   *
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  @Override
  public boolean last() {
    currentPos = items.size() - 1;
    return isPositionValid();
  }

  /**
   * Moves the cursor forward or backward from the current position by the given {@code amount}.
   *
   * @param amount The amount of results to move.
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  @Override
  public boolean relative(long amount) {
    currentPos += amount;
    return isPositionValid();
  }

  /**
   * Moves the cursor to the given position in the result-set. Setting the position to a negative number (for example to {@code -1}) has
   * the same effect as calling {@link #beforeFirst()}. Moving to any position being behind the last result has the same effect as calling
   * {@link #afterLast()}.
   *
   * @param position The absolute position with the first result being at position zero.
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  @Override
  public boolean absolute(long position) {
    if (position < -1) {
      beforeFirst();
    } else if (position > items.size()) {
      afterLast();
    } else {
      currentPos = (int) position;
    }
    return isPositionValid();
  }

  /**
   * Returns the operation that was executed.
   *
   * @return the operation that was executed.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public @NotNull EExecutedOp getOp() throws NoSuchElementException {
    return null;
  }

  /**
   * Returns the "type" from the root of the feature. When the feature follows the standard, the value will be "Feature".
   *
   * @return the "type" from the root of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public @NotNull String getFeatureType() throws NoSuchElementException {
    return "Feature";
  }

  /**
   * Returns the "type" from the "properties" of the feature.
   *
   * @return the "type" from the "properties" of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public @Nullable String getPropertiesType() throws NoSuchElementException {
    throw new NotImplementedException();
  }

  /**
   * Returns the "id" from the root of the feature.
   *
   * @return the "id" from the root of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public @NotNull String getId() throws NoSuchElementException {
    return getFeature().getId();
  }

  /**
   * Returns the "uuid" from the XYZ namespace of the feature.
   *
   * @return the "uuid" from the XYZ namespace of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public @NotNull String getUuid() throws NoSuchElementException {
    throw new NotImplementedException();
  }

  /**
   * Returns the raw JSON string of the feature without the geometry.
   *
   * @return the raw JSON string of the feature without the geometry.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public @Nullable String getJson() throws NoSuchElementException {
    throw new NotImplementedException();
  }

  /**
   * Returns the geometry as raw WKB. This value is always returned from cache and must not be modified to avoid cache pollution.
   *
   * @return the geometry as raw WKB.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public byte @Nullable [] rawGeometry() throws NoSuchElementException {
    throw new NotImplementedException();
  }

  /**
   * Returns the JTS geometry.
   *
   * @return the JTS geometry.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public @Nullable Geometry getGeometry() throws NoSuchElementException {
    throw new NotImplementedException();
  }

  /**
   * Returns the full feature including the geometry as POJO.
   *
   * @return the full feature including the geometry as POJO.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public @Nullable T getFeature() throws NoSuchElementException {
    if (!isPositionValid()) {
      throw new NoSuchElementException();
    }
    return featureType.cast(items.get(currentPos));
  }

  /**
   * Returns the full feature including the geometry as POJO. This method forces the parse to explicitly produce the given feature-class.
   *
   * @param featureClass The class of the feature-type to return.
   * @return the full feature including the geometry as POJO.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public <NT> @Nullable NT getFeature(@NotNull Class<NT> featureClass) throws NoSuchElementException {
    return withType(featureClass).getFeature();
  }

  /**
   * Returns this cursor as iterator over the selected feature-type {@code T}. Beware that the returned iterator is not only <b>not</b>
   * thread-safe, but actually will modify the cursor! Therefore, this is possible:
   * <pre>{@code
   * final ResultCursor<XyzFeature> cursor = ...;
   * for (final XyzFeature feature : cursor) {
   *   // This works, because the iterator does move the cursor!
   *   if (cursor.getGeometry() = null) {
   *   }
   *   // If the previous feature should be processed again.
   *   if (someCondition) {
   *     cursor.previous();
   *     continue;
   *   }
   * }
   * }</pre>
   *
   * @return an iterator to iterate above the features of the result-set, starting at the current cursor position and moving the cursor
   * forward; so the iterator will modify the underlying cursor.
   */
  @Override
  public @NotNull Iterator<@Nullable T> iterator() {
    return new Iterator<T>() {
      @Override
      public boolean hasNext() {
        return MockResultCursor.this.hasNext();
      }

      @Override
      public T next() {
        MockResultCursor.this.next();
        return MockResultCursor.this.getFeature();
      }
    };
  }

  /**
   * Close the cursor and drop all resources allocated for it.
   */
  @Override
  public void close() {}

  protected boolean isPositionValid() {
    return currentPos >= 0 && currentPos < items.size();
  }
}
