/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.Payload;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.responses.changesets.Changeset;
import com.here.xyz.responses.changesets.ChangesetCollection;
import com.here.xyz.responses.changesets.CompactChangeset;
import com.here.xyz.responses.maintenance.ConnectorStatus;
import com.here.xyz.responses.maintenance.SpaceStatus;

/**
 * All classes that represent a valid response of any remote procedure to the XYZ Hub need to extend this class.
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
    @JsonSubTypes.Type(value = FeatureCollection.class, name = "FeatureCollection"),
    @JsonSubTypes.Type(value = BinResponse.class, name = "BinResponse"),
    @JsonSubTypes.Type(value = Changeset.class, name = "Changeset"),
    @JsonSubTypes.Type(value = CompactChangeset.class, name = "CompactChangeset"),
    @JsonSubTypes.Type(value = ChangesetCollection.class, name = "ChangesetCollection"),
    @JsonSubTypes.Type(value = ConnectorStatus.class, name = "ConnectorStatus"),
    @JsonSubTypes.Type(value = SpaceStatus.class, name = "SpaceStatus")
})
public abstract class XyzResponse<T extends XyzResponse> extends Payload {

  private String etag;

  /**
   * An optional set e-tag which should be some value that allows the storage to check if the content of the response has changed.
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

  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public T withEtag(String etag) {
    setEtag(etag);
    //noinspection unchecked
    return (T) this;
  }
}
