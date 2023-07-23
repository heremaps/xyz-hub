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
package com.here.naksha.lib.core.models.payload;

import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileResponseType;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A response type as a hint what response(s) are acceptable by the client.
 */
public class XyzResponseType implements CharSequence {

  public XyzResponseType() {
    this(null, null, false);
  }

  public XyzResponseType(@Nullable CharSequence contentType) {
    this(contentType, null, false);
  }

  public XyzResponseType(
      @Nullable CharSequence contentType, @Nullable GetFeaturesByTileResponseType tileResponseType) {
    this(contentType, tileResponseType, false);
  }

  public XyzResponseType(
      @Nullable CharSequence contentType,
      @Nullable GetFeaturesByTileResponseType tileResponseType,
      boolean binary) {
    this.binary = binary;
    this.tileResponseType = tileResponseType;
    this.contentType = contentType != null ? contentType.toString() : null;
  }

  /**
   * If the response type is a binary; otherwise it is a text.
   */
  public final boolean binary;

  /**
   * The content-type.
   */
  public final @Nullable String contentType;

  /**
   * The content-type as added into the event.
   */
  public final @Nullable GetFeaturesByTileResponseType tileResponseType;

  @Override
  public int length() {
    return contentType != null ? contentType.length() : 0;
  }

  @Override
  public char charAt(int index) {
    if (index < 0 || contentType == null || index >= contentType.length()) {
      throw new IndexOutOfBoundsException(index);
    }
    return contentType.charAt(index);
  }

  @Override
  public @NotNull CharSequence subSequence(int start, int end) {
    return contentType != null ? contentType.subSequence(start, end) : EMPTY_STRING;
  }

  @Override
  public @NotNull String toString() {
    return contentType != null ? contentType : EMPTY_STRING;
  }

  /**
   * Constant for an empty string.
   */
  protected static final String EMPTY_STRING = "";
}
