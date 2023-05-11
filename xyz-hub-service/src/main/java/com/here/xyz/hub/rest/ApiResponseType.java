/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

package com.here.xyz.hub.rest;

import com.here.xyz.events.feature.GetFeaturesByTileResponseType;

import static com.here.xyz.hub.rest.HeaderValues.APPLICATION_GEO_JSON;
import static com.here.xyz.hub.rest.HeaderValues.APPLICATION_JSON;
import static com.here.xyz.hub.rest.HeaderValues.APPLICATION_VND_MAPBOX_VECTOR_TILE;

import com.here.xyz.responses.ErrorResponse;
import io.vertx.ext.web.MIMEHeader;
import io.vertx.ext.web.impl.ParsableMIMEValue;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * An enumeration with all responses that should be returned to the client. If the required response type is not available an
 * {@link ErrorResponse} with content-type {@link HeaderValues#APPLICATION_JSON} should be returned.
 */
public enum ApiResponseType implements CharSequence {
  EMPTY,
  ERROR(APPLICATION_JSON),
  FEATURE(APPLICATION_GEO_JSON, GetFeaturesByTileResponseType.GEO_JSON),
  FEATURE_COLLECTION(APPLICATION_GEO_JSON, GetFeaturesByTileResponseType.GEO_JSON),
  COMPACT_CHANGESET(APPLICATION_JSON),
  CHANGESET_COLLECTION(APPLICATION_JSON),
  MVT(APPLICATION_VND_MAPBOX_VECTOR_TILE, GetFeaturesByTileResponseType.MVT, true),
  MVT_FLATTENED(APPLICATION_VND_MAPBOX_VECTOR_TILE, GetFeaturesByTileResponseType.MVT_FLATTENED, true),
  SPACE(APPLICATION_JSON),
  SPACE_LIST(APPLICATION_JSON),
  @Deprecated
  COUNT_RESPONSE(APPLICATION_JSON),
  HEALTHY_RESPONSE(APPLICATION_JSON),
  STATISTICS_RESPONSE(APPLICATION_JSON),
  HISTORY_STATISTICS_RESPONSE(APPLICATION_JSON);

  /**
   * If the response type is a binary; otherwise it is a text.
   */
  public final boolean binary;

  /**
   * The content-type.
   */
  public final @Nullable String contentType;

  /**
   * The MIME header.
   */
  public final @Nullable MIMEHeader mimeType;

  /**
   * The content-type as added into the event.
   */
  public final @Nullable GetFeaturesByTileResponseType tileResponseType;

  ApiResponseType() {
    this.binary = false;
    this.tileResponseType = null;
    this.contentType = null;
    this.mimeType = null;
  }

  ApiResponseType(@NotNull CharSequence contentType) {
    this.binary = false;
    this.tileResponseType = null;
    this.contentType = contentType.toString();
    this.mimeType = new ParsableMIMEValue(this.contentType).forceParse();
  }

  ApiResponseType(@NotNull CharSequence contentType, @NotNull GetFeaturesByTileResponseType tileResponseType) {
    this.binary = false;
    this.tileResponseType = tileResponseType;
    this.contentType = contentType.toString();
    this.mimeType = new ParsableMIMEValue(this.contentType).forceParse();
    ;
  }

  ApiResponseType(@NotNull CharSequence contentType, @NotNull GetFeaturesByTileResponseType tileResponseType, boolean binary) {
    this.binary = binary;
    this.tileResponseType = tileResponseType;
    this.contentType = contentType.toString();
    this.mimeType = new ParsableMIMEValue(this.contentType).forceParse();
    ;
  }

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

  @Nonnull
  @Override
  public @NotNull CharSequence subSequence(int start, int end) {
    return contentType != null ? contentType.subSequence(start, end) : EMPTY_STRING;
  }

  @Override
  public @NotNull String toString() {
    return contentType != null ? contentType : EMPTY_STRING;
  }

  private static final String EMPTY_STRING = "";
}
