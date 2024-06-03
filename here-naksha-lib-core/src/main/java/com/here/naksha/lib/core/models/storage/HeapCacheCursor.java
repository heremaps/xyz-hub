/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * HeapCacheCursor reads all data (up to provided limit) ahead of time to memory, and that allows to go back and forth.
 * It's not thread-safe in any way, {@link SeekableCursor} is designed to jump to any position of the cursor, so it doesn't really make sense to use it in parallel.
 * Use {@link HeapCacheCursor} only when you need to walk through results multiple times, if you need to read data only once - please use {@link ForwardCursor} as it consumes much less memory.
 *
 * @param <FEATURE>
 * @param <CODEC>
 */
public class HeapCacheCursor<FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>>
    extends MutableCursor<FEATURE, CODEC> {

  protected static final long BEFORE_FIRST_POSITION = -1;
  protected static final int INITIAL_ARRAY_SIZE_FOR_UNLIMITED_CURSOR = 126;

  protected List<FeatureCodec<FEATURE, ?>> inMemoryData;

  protected ForwardCursor<?, ?> originalCursor;
  protected Map<String, Integer> originalFeaturesOrder;

  public HeapCacheCursor(
      @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory,
      @NotNull ForwardCursor<?, ?> originalCursor,
      @Nullable Map<String, Integer> originalFeaturesOrder) {
    super(codecFactory);
    this.originalCursor = originalCursor;
    this.originalFeaturesOrder = originalFeaturesOrder;
    this.position = BEFORE_FIRST_POSITION;
    this.inMemoryData = new ArrayList<>(INITIAL_ARRAY_SIZE_FOR_UNLIMITED_CURSOR);

    while (originalCursor.hasNext()) {
      originalCursor.next();
      CODEC codec = codecFactory.newInstance().withParts(originalCursor.currentRow.codec);
      inMemoryData.add(codec);
    }
  }

  public HeapCacheCursor(
      @NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory,
      @NotNull List<CODEC> result,
      @Nullable Map<String, Integer> originalFeaturesOrder) {
    super(codecFactory);
    this.originalCursor = null;
    this.originalFeaturesOrder = originalFeaturesOrder;
    this.position = BEFORE_FIRST_POSITION;
    this.inMemoryData = new ArrayList<>(result);
  }

  @Override
  public boolean restoreInputOrder() {
    if (originalFeaturesOrder == null) {
      return false;
    }

    FeatureCodec<FEATURE, ?>[] inMemoryDataWithRestoredOrder = new FeatureCodec[inMemoryData.size()];
    for (FeatureCodec<FEATURE, ?> feature : inMemoryData) {
      Integer previousPosition = originalFeaturesOrder.get(feature.getId());
      if (previousPosition == null) {
        return false;
      }
      inMemoryDataWithRestoredOrder[previousPosition] = feature;
    }
    this.inMemoryData = Arrays.asList(inMemoryDataWithRestoredOrder);
    beforeFirst();
    return true;
  }

  @Override
  public boolean next() {
    if (hasNext()) {
      loadNextRow(currentRow);
      this.position++;
      return true;
    }
    return false;
  }

  @Override
  public boolean hasNext() {
    return this.position < positionOfLastElement();
  }

  @Override
  protected boolean loadNextRow(ForwardCursor<FEATURE, CODEC>.@NotNull Row row) {
    return loadPosition(row, position + 1);
  }

  @Override
  public void close() {
    if (originalCursor != null) {
      originalCursor.close();
      originalCursor = null;
    }
  }

  @Override
  public boolean previous() {
    return absolute(position - 1);
  }

  @Override
  public void beforeFirst() {
    this.currentRow.valid = false;
    this.position = BEFORE_FIRST_POSITION;
  }

  @Override
  public boolean first() {
    return absolute(0);
  }

  @Override
  public void afterLast() {
    this.position = positionOfLastElement() + 1;
    this.currentRow.valid = false;
  }

  @Override
  public boolean last() {
    return absolute(positionOfLastElement());
  }

  @Override
  public boolean relative(long shift) {
    return absolute(this.position + shift);
  }

  @Override
  public boolean absolute(long position) {
    if (position > positionOfLastElement()) {
      afterLast();
      return false;
    }
    if (position <= BEFORE_FIRST_POSITION) {
      beforeFirst();
      return false;
    }
    this.position = position;
    return loadPosition(currentRow, position);
  }

  @Override
  public @NotNull FEATURE addFeature(@NotNull FEATURE feature) {
    inMemoryData.add(createCodec(feature));
    this.originalFeaturesOrder = null;
    return feature;
  }

  @Override
  public @Nullable FEATURE setFeature(long position, @NotNull FEATURE feature) {
    int idx = toIdx(position);
    FeatureCodec<FEATURE, ?> currentPositionCodec = inMemoryData.get(idx);
    inMemoryData.set(toIdx(position), createCodec(feature));
    if (position == this.position) {
      loadPosition(currentRow, position);
    }
    this.originalFeaturesOrder = null;
    return requireNonNull(currentPositionCodec.encodeFeature(false).getFeature());
  }

  @Override
  public @Nullable FEATURE setFeature(@NotNull FEATURE feature) {
    return setFeature(this.position, feature);
  }

  @Override
  public @Nullable FEATURE removeFeature() {
    return requireNonNull(removeFeature(this.position));
  }

  @Override
  public @Nullable FEATURE removeFeature(long position) {
    int idx = toIdx(position);
    FeatureCodec<FEATURE, ?> featureToRemove = inMemoryData.get(idx);
    inMemoryData.remove(idx);
    if (position <= this.position) {
      previous();
    }
    return featureToRemove.encodeFeature(false).getFeature();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <NF, NC extends FeatureCodec<NF, NC>, N_CUR extends ForwardCursor<NF, NC>> @NotNull N_CUR withCodecFactory(
      @NotNull FeatureCodecFactory<NF, NC> codecFactory, boolean reEncode) {
    for (int i = 0; i < inMemoryData.size(); i++) {
      FeatureCodec<?, ?> currentCodec = inMemoryData.get(i);
      FeatureCodec<NF, NC> newCodec = codecFactory.newInstance();
      newCodec.withParts(currentCodec);
      if (reEncode) {
        newCodec.encodeFeature(true);
      }
      inMemoryData.set(i, (FeatureCodec<FEATURE, ?>) newCodec);
    }
    return (N_CUR) this;
  }

  public @Nullable ForwardCursor<?, ?> getOriginalCursor() {
    return originalCursor;
  }

  @Override
  public List<CODEC> asList() {
    return (List<CODEC>) inMemoryData;
  }

  private boolean loadPosition(ForwardCursor<FEATURE, CODEC>.@NotNull Row row, long positionToLoad) {
    row.codec = inMemoryData.get(toIdx(positionToLoad));
    row.valid = true;
    return true;
  }

  private int toIdx(long position) {
    if (position > positionOfLastElement()) {
      throw new NoSuchElementException("Position is over last element");
    }
    if (position <= BEFORE_FIRST_POSITION) {
      throw new NoSuchElementException("Position is before first element");
    }
    return toIntExact(position);
  }

  private int positionOfLastElement() {
    return this.inMemoryData.size() - 1;
  }

  private CODEC createCodec(FEATURE feature) {
    CODEC codec = codecFactory.newInstance();
    codec.setFeature(feature);
    return codec;
  }
}
