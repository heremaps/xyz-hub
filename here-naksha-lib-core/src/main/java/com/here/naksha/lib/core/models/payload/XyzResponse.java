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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeatureCollection;
import com.here.naksha.lib.core.models.payload.responses.CountResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.HealthStatus;
import com.here.naksha.lib.core.models.payload.responses.HistoryStatisticsResponse;
import com.here.naksha.lib.core.models.payload.responses.ModifiedEventResponse;
import com.here.naksha.lib.core.models.payload.responses.ModifiedResponseResponse;
import com.here.naksha.lib.core.models.payload.responses.NotModifiedResponse;
import com.here.naksha.lib.core.models.payload.responses.StatisticsResponse;
import com.here.naksha.lib.core.models.payload.responses.StorageStatistics;
import com.here.naksha.lib.core.models.payload.responses.SuccessResponse;
import com.here.naksha.lib.core.models.payload.responses.changesets.Changeset;
import com.here.naksha.lib.core.models.payload.responses.changesets.ChangesetCollection;
import com.here.naksha.lib.core.models.payload.responses.changesets.CompactChangeset;
import com.here.naksha.lib.core.models.payload.responses.maintenance.ConnectorStatus;
import com.here.naksha.lib.core.models.payload.responses.maintenance.SpaceStatus;
import com.here.naksha.lib.core.util.Hasher;
import com.here.naksha.lib.core.util.StringHelper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All classes that represent a valid response of any remote procedure to the XYZ Hub need to extend
 * this class.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({
  @JsonSubTypes.Type(value = CountResponse.class, name = "CountResponse"),
  @JsonSubTypes.Type(value = ErrorResponse.class, name = "ErrorResponse"),
  @JsonSubTypes.Type(value = HealthStatus.class, name = "HealthStatus"),
  @JsonSubTypes.Type(value = ModifiedEventResponse.class, name = "ModifiedEventResponse"),
  @JsonSubTypes.Type(value = ModifiedResponseResponse.class, name = "ModifiedResponseResponse"),
  @JsonSubTypes.Type(value = StatisticsResponse.class, name = "StatisticsResponse"),
  @JsonSubTypes.Type(value = StorageStatistics.class, name = "StorageStatistics"),
  @JsonSubTypes.Type(value = HistoryStatisticsResponse.class, name = "HistoryStatisticsResponse"),
  @JsonSubTypes.Type(value = SuccessResponse.class, name = "SuccessResponse"),
  @JsonSubTypes.Type(value = NotModifiedResponse.class, name = "NotModifiedResponse"),
  @JsonSubTypes.Type(value = XyzFeatureCollection.class, name = "FeatureCollection"),
  @JsonSubTypes.Type(value = Changeset.class, name = "Changeset"),
  @JsonSubTypes.Type(value = CompactChangeset.class, name = "CompactChangeset"),
  @JsonSubTypes.Type(value = ChangesetCollection.class, name = "ChangesetCollection"),
  @JsonSubTypes.Type(value = ConnectorStatus.class, name = "ConnectorStatus"),
  @JsonSubTypes.Type(value = SpaceStatus.class, name = "SpaceStatus")
})
public abstract class XyzResponse extends Payload {

  private String etag;

  /**
   * An optional set e-tag which should be some value that allows the storage to check if the
   * content of the response has changed.
   *
   * @return the e-tag, when it was calculated.
   */
  @SuppressWarnings("unused")
  public String getEtag() {
    return this.etag;
  }

  /**
   * Set the e-tag (a hash above all features), when it was calculated.
   *
   * @param etag the e-tag, if null, the e-tag is removed.
   */
  @SuppressWarnings("WeakerAccess")
  public void setEtag(String etag) {
    this.etag = etag;
  }

  /**
   * Calculate the hash of the given bytes. Returns a strong e-tag.
   *
   * @param bytes the bytes to hash.
   * @return the strong e-tag.
   */
  public static @NotNull String calculateEtagFor(byte @NotNull [] bytes) {
    return "\"" + Hasher.getHash(bytes) + "\"";
  }

  public static boolean etagMatches(@Nullable String ifNoneMatch, @Nullable String etag) {
    if (ifNoneMatch == null || ifNoneMatch.length() == 0 || etag == null || etag.length() == 0) return false;
    // Note: Be resilient against clients that do not follow the spec accordingly.
    //       Some clients may not add the quotation marks around e-tags or use weak e-tags.
    //       We currently treat weak and normal e-tags the same, so ignore W/ and leading/ending
    // quotes.
    // See:  https://en.wikipedia.org/wiki/HTTP_ETag
    int etagStart = 0;
    int etagEnd = etag.length();
    if (etag.startsWith("W/") || etag.startsWith("w/")) etagStart += 2;
    if (etagStart < etag.length() && etag.charAt(etagStart) == '"') etagStart += 1;
    if (etag.charAt(etagEnd - 1) == '"') etagEnd -= 1;

    int inmStart = 0;
    int inmEnd = ifNoneMatch.length();
    if (ifNoneMatch.startsWith("W/") || ifNoneMatch.startsWith("w/")) inmStart += 2;
    if (inmStart < ifNoneMatch.length() && ifNoneMatch.charAt(inmStart) == '"') inmStart += 1;
    if (ifNoneMatch.charAt(inmEnd - 1) == '"') inmEnd -= 1;
    return StringHelper.equals(etag, etagStart, etagEnd, ifNoneMatch, inmStart, inmEnd);
  }
}
