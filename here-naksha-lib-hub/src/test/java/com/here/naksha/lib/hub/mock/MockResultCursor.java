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
import com.here.naksha.lib.core.models.storage.ForwardCursor;
import com.here.naksha.lib.core.models.storage.XyzCodecFactory;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodec;
import com.here.naksha.lib.core.models.storage.XyzFeatureCodecFactory;
import com.vividsolutions.jts.geom.Geometry;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.NotImplementedException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

class MockResultCursor<T extends XyzFeature> extends ForwardCursor<XyzFeature, XyzFeatureCodec> {

  private final @NotNull Class<T> featureType;
  private final @NotNull List<XyzFeatureCodec> items;
  protected int currentPos;

  MockResultCursor(@NotNull Class<T> featureType, @NotNull List<XyzFeatureCodec> items) {
    super(XyzCodecFactory.getFactory(XyzFeatureCodecFactory.class));
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
   * Returns the operation that was executed.
   *
   * @return the operation that was executed.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  @Override
  public @NotNull EExecutedOp getOp() throws NoSuchElementException {
    if (isPositionValid()) {
      return EExecutedOp.get(items.get(currentPos).getOp());
    }
    throw new NoSuchElementException();
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
    if (!isPositionValid()) {
      throw new NoSuchElementException();
    }
    return items.get(currentPos).getId();
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
    return featureType.cast(items.get(currentPos).getFeature());
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
  public @NotNull Iterator<@Nullable XyzFeature> iterator() {
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return MockResultCursor.this.hasNext();
      }

      @Override
      public XyzFeature next() {
        MockResultCursor.this.next();
        return MockResultCursor.this.getFeature();
      }
    };
  }

  /**
   * Loads the next row into the given one.
   *
   * @param row
   * @return {@code true} if the next row was loaded successfully and is valid; {@code false} if there is no more row, the given row should
   * be invalid.
   */
  @Override
  protected boolean loadNextRow(@NotNull ForwardCursor.Row row) {
    throw new NotImplementedException("Not needed in mock");
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
