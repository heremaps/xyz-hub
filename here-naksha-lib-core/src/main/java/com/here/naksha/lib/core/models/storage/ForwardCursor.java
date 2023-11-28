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

import com.vividsolutions.jts.geom.Geometry;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A simple cursor that allows to read the results forward only. This class comes with most of the needed default methods.
 *
 * @param <FEATURE> The feature type that the cursor returns.
 * @param <CODEC>   The codec type.
 */
@SuppressWarnings("unused")
public abstract class ForwardCursor<FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>>
    implements Iterable<FEATURE>, AutoCloseable {

  /**
   * Creates a new forward cursor.
   *
   * @param codecFactory The codec factory to use.
   */
  protected ForwardCursor(@NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory) {
    this.codecFactory = codecFactory;
    this.position = -1L;
    this.currentRow = newRow();
    this.nextRow = newRow();
  }

  /**
   * Called to create new rows. If a special row type is needed, can be overridden.
   */
  protected @NotNull Row newRow() {
    return new Row();
  }

  /**
   * The internally used codec factory.
   */
  protected @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory;

  /**
   * If features should be re-encoded, when the codec factory is changed.
   */
  protected boolean reEncode = true;

  /**
   * Returns the currently used codec factory.
   *
   * @return the currently used codec factory.
   */
  public @NotNull FeatureCodecFactory<FEATURE, CODEC> getCodecFactory() {
    return codecFactory;
  }

  /**
   * Replaces the current codec factory with another implementation producing the same feature type.
   *
   * @param codecFactory The new codec factory to use.
   * @param reEncode     If already encoded features should be re-encoded using the new factory.
   * @return The previously used codec factory.
   */
  public @NotNull FeatureCodecFactory<FEATURE, CODEC> setCodecFactory(
      @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory, boolean reEncode) {
    final FeatureCodecFactory<FEATURE, CODEC> old = this.codecFactory;
    this.codecFactory = codecFactory;
    this.reEncode = reEncode;
    return old;
  }

  /**
   * Replaces the current codec factory with another implementation producing a new feature type.
   *
   * @param codecFactory The new codec factory to use.
   * @param reEncode     If already encoded features should be re-encoded using the new factory.
   * @param <NF>         The new feature type to use.
   * @param <NC>         The new codec type to use.
   * @return this with changed feature type.
   */
  @SuppressWarnings("unchecked")
  public <NF, NC extends FeatureCodec<NF, NC>> @NotNull ForwardCursor<NF, NC> withCodecFactory(
      @NotNull FeatureCodecFactory<NF, NC> codecFactory, boolean reEncode) {
    this.codecFactory = (FeatureCodecFactory<FEATURE, CODEC>) codecFactory;
    this.reEncode = reEncode;
    if (reEncode) {
      this.currentRow.codec = ForwardCursor.this.codecFactory.newInstance();
      this.nextRow.codec = ForwardCursor.this.codecFactory.newInstance();
    }
    return (ForwardCursor<NF, NC>) this;
  }

  /**
   * The iterator being used, when iterating this cursor.
   */
  protected final @NotNull Iterator<FEATURE> iterator = new Iterator<>() {
    @Override
    public boolean hasNext() {
      return ForwardCursor.this.hasNext();
    }

    @Override
    public FEATURE next() {
      if (!ForwardCursor.this.next()) {
        throw new NoSuchElementException();
      }
      return ForwardCursor.this.getFeature();
    }
  };

  /**
   * Returns this cursor as iterator over the selected feature-type. Beware that the returned iterator is <b>not</b> thread-safe, but
   * actually will modify the underlying cursor! Therefore, this is possible:
   * <pre>{@code
   * final ForwardCursor<XyzFeature> cursor = ...;
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
  public @NotNull Iterator<@NotNull FEATURE> iterator() {
    return iterator;
  }

  /**
   * The row fetched from the storage.
   */
  protected class Row {

    /**
     * Create a new empty row.
     */
    protected Row() {
      codec = ForwardCursor.this.codecFactory.newInstance();
      clear();
    }

    /**
     * Create a clone of the given row. This can be used when caching rows.
     *
     * @param other The row to copy.
     */
    protected Row(@NotNull Row other) {
      codec = ForwardCursor.this.codecFactory.newInstance();
      codec.withParts(other.codec);
      codec.setFeature(other.codec.getFeature());
      valid = other.valid;
    }

    /**
     * Clear the row and flag it as invalid (prepare loading new values).
     *
     * @return this.
     */
    public @NotNull Row clear() {
      codec.clear();
      valid = false;
      return this;
    }

    /**
     * Load the given values into this row and its codec. If the codec does not match the one of the cursor, re-create the codec.
     *
     * @return this.
     */
    public @NotNull Row load(
        @NotNull String op,
        @NotNull String id,
        @NotNull String uuid,
        @NotNull String typeId,
        @Nullable String propertiesTypeId,
        @NotNull String json,
        byte @Nullable [] wkb) {
      if (!ForwardCursor.this.codecFactory.isInstance(codec)) {
        codec = ForwardCursor.this.codecFactory.newInstance();
      } else {
        codec.clear();
      }
      codec.setOp(EExecutedOp.get(EExecutedOp.class, op));
      codec.setId(id);
      codec.setUuid(uuid);
      codec.setFeatureType(typeId);
      codec.setPropertiesType(propertiesTypeId);
      codec.setJson(json);
      codec.setWkb(wkb);
      return this;
    }

    /**
     * The reference to the codec used to generate the {@code feature} and {@code geometry} from the raw data. It as well holds the raw data
     * loaded from the underlying storage.
     */
    public @NotNull FeatureCodec<FEATURE, ?> codec;

    /**
     * If this row is valid (does exist and is loaded).
     */
    public boolean valid;
  }

  /**
   * Loads the next row into the given one.
   *
   * @return {@code true} if the next row was loaded successfully and is valid; {@code false} if there is no more row, the given row should
   * be invalid.
   */
  protected abstract boolean loadNextRow(@NotNull Row row);

  /**
   * The current position, where {@code -1} is before the first result.
   */
  protected long position;

  /**
   * The row at {@link #position}.
   */
  protected @NotNull Row currentRow;

  /**
   * The row at {@link #position} + 1.
   */
  protected @NotNull Row nextRow;

  /**
   * If the next row is load.
   */
  protected boolean nextRowLoaded;

  /**
   * Tests whether there is another result.
   *
   * @return {@code true} if another result is available; {@code false} otherwise.
   */
  public boolean hasNext() {
    if (!nextRowLoaded) {
      nextRowLoaded = true;
      return loadNextRow(nextRow);
    }
    return nextRow.valid;
  }

  /**
   * Moves the cursor to the next result.
   *
   * @return {@code true}, if the cursor is on a valid result; {@code false} otherwise.
   */
  public boolean next() {
    if (!nextRowLoaded) {
      nextRowLoaded = true;
      if (!loadNextRow(nextRow)) {
        throw new NoSuchElementException();
      }
    }
    if (!nextRow.valid) {
      throw new NoSuchElementException();
    }
    // Move the next row into the current row and make the next row invalid and unloaded.
    position++;
    final Row tmp = currentRow;
    currentRow = nextRow;
    nextRow = tmp;
    nextRow.clear();
    nextRowLoaded = false;
    return true;
  }

  /**
   * Returns the operation that was executed.
   *
   * @return the operation that was executed.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public @NotNull EExecutedOp getOp() throws NoSuchElementException {
    if (!currentRow.valid) {
      throw new NoSuchElementException();
    }
    return EExecutedOp.get(currentRow.codec.getOp());
  }

  /**
   * Returns the "type" from the root of the feature. When the feature follows the standard, the value will be "Feature".
   *
   * @return the "type" from the root of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public @NotNull String getFeatureType() throws NoSuchElementException {
    if (!currentRow.valid) {
      throw new NoSuchElementException();
    }
    String typeId = currentRow.codec.getFeatureType();
    assert typeId != null;
    return typeId;
  }

  /**
   * Returns the "type" from the "properties" of the feature.
   *
   * @return the "type" from the "properties" of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public @Nullable String getPropertiesType() throws NoSuchElementException {
    if (!currentRow.valid) {
      throw new NoSuchElementException();
    }
    return currentRow.codec.getPropertiesType();
  }

  /**
   * Returns the "id" from the root of the feature.
   *
   * @return the "id" from the root of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public @NotNull String getId() throws NoSuchElementException {
    if (!currentRow.valid) {
      throw new NoSuchElementException();
    }
    String id = currentRow.codec.getId();
    assert id != null;
    return id;
  }

  /**
   * Returns the "uuid" from the XYZ namespace of the feature.
   *
   * @return the "uuid" from the XYZ namespace of the feature.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public @NotNull String getUuid() throws NoSuchElementException {
    if (!currentRow.valid) {
      throw new NoSuchElementException();
    }
    String uuid = currentRow.codec.getUuid();
    assert uuid != null;
    return uuid;
  }

  /**
   * Returns the raw JSON string of the feature without the geometry.
   *
   * @return the raw JSON string of the feature without the geometry.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public @NotNull String getJson() throws NoSuchElementException {
    if (!currentRow.valid) {
      throw new NoSuchElementException();
    }
    String json = currentRow.codec.getJson();
    assert json != null;
    return json;
  }

  /**
   * Returns the geometry as raw WKB. This value is always returned from cache and must not be modified to avoid cache pollution.
   *
   * @return the geometry as raw WKB.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public byte @Nullable [] getWkb() throws NoSuchElementException {
    if (!currentRow.valid) {
      throw new NoSuchElementException();
    }
    return currentRow.codec.getWkb();
  }

  /**
   * Returns the JTS geometry.
   *
   * @return the JTS geometry.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public @Nullable Geometry getGeometry() throws NoSuchElementException {
    if (!currentRow.valid) {
      throw new NoSuchElementException();
    }
    return currentRow.codec.getGeometry();
  }

  /**
   * Returns the full feature including the geometry as POJO.
   *
   * @return the full feature including the geometry as POJO.
   * @throws NoSuchElementException If the cursor currently is not at a valid result.
   */
  public @NotNull FEATURE getFeature() throws NoSuchElementException {
    if (!currentRow.valid) {
      throw new NoSuchElementException();
    }
    final FeatureCodec<FEATURE, ?> codec = currentRow.codec;
    FEATURE feature = codec.encodeFeature(false).getFeature();
    assert feature != null;
    return feature;
  }

  /**
   * Returns true if current row has error, otherwise false.
   * @return
   */
  public boolean hasError() {
    return currentRow.codec.hasError();
  }

  /**
   * Returns current row error or null.
   * @return
   */
  public @Nullable CodecError getError() {
    return currentRow.codec.getError();
  }

  public @NotNull MutableCursor<FEATURE, CODEC> toMutableCursor(long limit, boolean reOrder) {
    throw new UnsupportedOperationException();
  }

  public @NotNull SeekableCursor<FEATURE, CODEC> asSeekableCursor(long limit, boolean reOrder) {
    throw new UnsupportedOperationException();
  }

  /**
   * Close the cursor and drop all resources allocated for it.
   */
  @Override
  public abstract void close();
}
