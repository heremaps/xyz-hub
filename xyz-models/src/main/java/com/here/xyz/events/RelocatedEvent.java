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

package com.here.xyz.events;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * If this event is received, then the serialized real event was too big to be send to the storage provider and therefore it was relocated
 * into a cache and must be read from the cache.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "RelocatedEvent")
public final class RelocatedEvent extends Event<RelocatedEvent> {

  @Deprecated
  protected static final String LOCATION = "location";

  @JsonProperty("URI")
  private String URI;
  @Deprecated
  private String location;
  private String region;

  /**
   * Get the URI of the relocated event.
   *
   * The URI must be a valid Amazon S3 URI(s3://...) or an HTTP URI( http://... )
   *
   * @return the URI
   */
  public String getURI() {
    return this.URI;
  }

  /**
   * Set the URI of the relocated event.
   *
   * @param URI must be a valid Amazon S3 URI(s3://...) or an HTTP URI( http://... )
   */
  public void setURI(String URI) {
    this.URI = URI;
  }

  @SuppressWarnings("unused")
  public RelocatedEvent withURI(String URI) {
    setURI(URI);
    return this;
  }

  public String getRegion() {
    return region;
  }

  public void setRegion(String region) {
    this.region = region;
  }

  public RelocatedEvent withRegion(String region) {
    setRegion(region);
    return this;
  }

  @Deprecated
  public String getLocation() {
    return this.location;
  }

  @Deprecated
  public void setLocation(String location) {
    this.location = location;
  }

  @SuppressWarnings("unused")
  @Deprecated
  public RelocatedEvent withLocation(String location) {
    setLocation(location);
    return this;
  }
}
