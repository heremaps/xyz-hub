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

import com.here.naksha.lib.core.NakshaVersion;
import com.vividsolutions.jts.geom.Geometry;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Results produces by read- or write-requests. Beware that all results returned by this cursor are internally cached and must not be
 * modified to avoid cache pollution.
 *
 * @param <T> The feature-type to be produced; if fixed.
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_7)
public abstract class ResultCursor<T> implements AutoCloseable, Iterable<T> {

  /**
   * Switches to feature-type auto-detection, this means each result can differ. Requires the client to test for types.
   *
   * @return this.
   */
  public @NotNull ResultCursor<?> withAutoType() {
    this.featureClass = null;
    return this;
  }

  /**
   * Fix the feature-type, forcing the parser to produce this type.
   *
   * @param featureClass The class of the feature-type.
   * @param <NT>         the feature-type to produce.
   * @return this.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <NT> @NotNull ResultCursor<NT> withType(@Nullable Class<NT> featureClass) {
    this.featureClass = (Class) featureClass;
    return (ResultCursor<NT>) this;
  }

  /**
   * The default-feature type; if any. If {@code null}, then the type should be auto-detected by the JSON parser.
   */
  protected @Nullable Class<T> featureClass;

  /**
   * Tests whether there is another result.
   *
   * @return {@code true} if another result is available; {@code false} otherwise.
   */
  public abstract boolean hasNext();

  /**
   * Moves the cursor to the next result.
   *
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  public abstract boolean next();

  /**
   * Moves the cursor to the previous result.
   *
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  public abstract boolean previous();

  /**
   * Moves the cursor before the first result, so that calling {@link #next()} will load the first result.
   */
  public abstract void beforeFirst();

  /**
   * Moves the cursor to the first result.
   *
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  public abstract boolean first();

  /**
   * Moves the cursor behind the last result, so that calling {@link #previous()} will load the last result.
   */
  public abstract void afterLast();

  /**
   * Moves the cursor to the last result.
   *
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  public abstract boolean last();

  /**
   * Moves the cursor forward or backward from the current position by the given {@code amount}.
   *
   * @param amount The amount of results to move.
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  public abstract boolean relative(long amount);

  /**
   * Moves the cursor to the given position in the result-set. Setting the position to a negative number (for example to {@code -1}) has the
   * same effect as calling {@link #beforeFirst()}. Moving to any position being behind the last result has the same effect as calling
   * {@link #afterLast()}.
   *
   * @param position The absolute position with the first result being at position zero.
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  public abstract boolean absolute(long position);

  /**
   * Returns the operation that was executed.
   *
   * @return the operation that was executed.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract @NotNull EExecutedOp getOp() throws NoSuchElementException;

  /**
   * Returns the "type" from the root of the feature. When the feature follows the standard, the value will be "Feature".
   *
   * @return the "type" from the root of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract @NotNull String getFeatureType() throws NoSuchElementException;

  /**
   * Returns the "type" from the "properties" of the feature.
   *
   * @return the "type" from the "properties" of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract @Nullable String getPropertiesType() throws NoSuchElementException;

  /**
   * Returns the "id" from the root of the feature.
   *
   * @return the "id" from the root of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract @NotNull String getId() throws NoSuchElementException;

  /**
   * Returns the "uuid" from the XYZ namespace of the feature.
   *
   * @return the "uuid" from the XYZ namespace of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract @NotNull String getUuid() throws NoSuchElementException;

  /**
   * Returns the raw JSON string of the feature without the geometry.
   *
   * @return the raw JSON string of the feature without the geometry.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract @Nullable String getJson() throws NoSuchElementException;

  /**
   * Returns the geometry as raw WKB. This value is always returned from cache and must not be modified to avoid cache pollution.
   *
   * @return the geometry as raw WKB.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract byte @Nullable [] rawGeometry() throws NoSuchElementException;

  /**
   * Returns the JTS geometry.
   *
   * @return the JTS geometry.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract @Nullable Geometry getGeometry() throws NoSuchElementException;

  /**
   * Returns the full feature including the geometry as POJO.
   *
   * @return the full feature including the geometry as POJO.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract @Nullable T getFeature() throws NoSuchElementException;

  /**
   * Returns the full feature including the geometry as POJO. This method forces the parse to explicitly produce the given feature-class.
   *
   * @param featureClass The class of the feature-type to return.
   * @return the full feature including the geometry as POJO.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public abstract <NT> @Nullable NT getFeature(@NotNull Class<NT> featureClass) throws NoSuchElementException;

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
  public abstract @NotNull Iterator<@Nullable T> iterator();

  /**
   * Close the cursor and drop all resources allocated for it.
   */
  @Override
  public abstract void close();
}
