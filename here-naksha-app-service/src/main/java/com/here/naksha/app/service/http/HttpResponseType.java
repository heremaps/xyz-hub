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
package com.here.naksha.app.service.http;

import com.here.naksha.lib.core.models.payload.XyzResponseType;
import com.here.naksha.lib.core.models.payload.events.feature.GetFeaturesByTileResponseType;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.util.MIMEType;
import io.vertx.ext.web.MIMEHeader;
import io.vertx.ext.web.impl.ParsableMIMEValue;
import org.jetbrains.annotations.Nullable;

/**
 * An enumeration with all responses that should be returned to the client. If the required response type is not available an
 * {@link ErrorResponse} with content-type {@link MIMEType#APPLICATION_JSON} should be returned.
 */
@SuppressWarnings("unused")
public class HttpResponseType extends XyzResponseType {

  public static final HttpResponseType EMPTY = new HttpResponseType(MIMEType.APPLICATION_X_EMPTY);
  public static final HttpResponseType ERROR = new HttpResponseType(MIMEType.APPLICATION_JSON);
  public static final HttpResponseType FEATURE =
      new HttpResponseType(MIMEType.APPLICATION_GEO_JSON, GetFeaturesByTileResponseType.GEO_JSON);
  public static final HttpResponseType FEATURE_COLLECTION =
      new HttpResponseType(MIMEType.APPLICATION_GEO_JSON, GetFeaturesByTileResponseType.GEO_JSON);
  public static final HttpResponseType COMPACT_CHANGESET = new HttpResponseType(MIMEType.APPLICATION_JSON);
  public static final HttpResponseType CHANGESET_COLLECTION = new HttpResponseType(MIMEType.APPLICATION_JSON);
  public static final HttpResponseType MVT =
      new HttpResponseType(MIMEType.APPLICATION_VND_MAPBOX_VECTOR_TILE, GetFeaturesByTileResponseType.MVT, true);
  public static final HttpResponseType MVT_FLATTENED = new HttpResponseType(
      MIMEType.APPLICATION_VND_MAPBOX_VECTOR_TILE, GetFeaturesByTileResponseType.MVT_FLATTENED, true);
  public static final HttpResponseType SPACE = new HttpResponseType(MIMEType.APPLICATION_JSON);
  public static final HttpResponseType SPACE_LIST = new HttpResponseType(MIMEType.APPLICATION_JSON);

  @Deprecated
  public static final HttpResponseType COUNT_RESPONSE = new HttpResponseType(MIMEType.APPLICATION_JSON);

  public static final HttpResponseType HEALTHY_RESPONSE = new HttpResponseType(MIMEType.APPLICATION_JSON);
  public static final HttpResponseType STATISTICS_RESPONSE = new HttpResponseType(MIMEType.APPLICATION_JSON);
  public static final HttpResponseType HISTORY_STATISTICS_RESPONSE = new HttpResponseType(MIMEType.APPLICATION_JSON);

  /**
   * The MIME header.
   */
  public final @Nullable MIMEHeader mimeType;

  HttpResponseType() {
    this.mimeType = this.contentType != null ? new ParsableMIMEValue(this.contentType).forceParse() : null;
  }

  HttpResponseType(@Nullable CharSequence contentType) {
    super(contentType);
    this.mimeType = this.contentType != null ? new ParsableMIMEValue(this.contentType).forceParse() : null;
  }

  HttpResponseType(@Nullable CharSequence contentType, @Nullable GetFeaturesByTileResponseType tileResponseType) {
    super(contentType, tileResponseType);
    this.mimeType = this.contentType != null ? new ParsableMIMEValue(this.contentType).forceParse() : null;
  }

  HttpResponseType(
      @Nullable CharSequence contentType,
      @Nullable GetFeaturesByTileResponseType tileResponseType,
      boolean binary) {
    super(contentType, tileResponseType, binary);
    this.mimeType = this.contentType != null ? new ParsableMIMEValue(this.contentType).forceParse() : null;
  }
}
