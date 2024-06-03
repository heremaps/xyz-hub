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

import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * A cursor that where navigation in the result-set is possible.
 *
 * @param <FEATURE> The feature type that the cursor returns.
 * @param <CODEC> The codec type.
 */
public abstract class SeekableCursor<FEATURE, CODEC extends FeatureCodec<FEATURE, CODEC>>
    extends ForwardCursor<FEATURE, CODEC> {

  /**
   * Creates a new seekable cursor.
   *
   * @param codecFactory The codec factory to use.
   */
  protected SeekableCursor(@NotNull FeatureCodecFactory<FEATURE, CODEC> codecFactory) {
    super(codecFactory);
  }

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

  public abstract List<CODEC> asList();
}
