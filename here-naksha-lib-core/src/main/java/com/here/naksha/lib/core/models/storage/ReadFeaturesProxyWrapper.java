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

import java.util.HashMap;
import java.util.Map;

public class ReadFeaturesProxyWrapper extends ReadFeatures {

  private ReadRequestType readRequestType;

  private Map<String, Object> queryParameters;

  public enum ReadRequestType {
    GET_BY_ID,
    GET_BY_IDS,
    GET_BY_BBOX,
    GET_BY_TILE,
    ITERATE
  }

  public ReadFeaturesProxyWrapper() {
    super();
    this.queryParameters = new HashMap<>();
  }

  public ReadRequestType getReadRequestType() {
    return readRequestType;
  }

  public ReadFeaturesProxyWrapper withReadRequestType(ReadRequestType requestType) {
    this.readRequestType = requestType;
    return this;
  }

  public Map<String, Object> getQueryParameters() {
    return queryParameters;
  }

  public <T> T getQueryParameter(String key) throws ClassCastException {
    return (T) queryParameters.get(key);
  }

  public ReadFeaturesProxyWrapper withQueryParameters(Map<String, Object> parameters) {
    this.queryParameters = parameters;
    return this;
  }

  @Override
  public ReadFeaturesProxyWrapper shallowClone() {
    ReadFeaturesProxyWrapper clone = new ReadFeaturesProxyWrapper();
    // ReadFeatures fields
    clone.setPropertyOp(this.getPropertyOp());
    clone.setCollections(this.getCollections());
    clone.setSpatialOp(this.getSpatialOp());
    clone.limit = this.limit;
    clone.returnDeleted = this.returnDeleted;
    clone.withReturnAllVersions(isReturnAllVersions());
    clone.fetchSize = this.fetchSize;

    // ReadFeaturesProxyWrapper fields
    clone.withReadRequestType(this.getReadRequestType());
    clone.withQueryParameters(this.getQueryParameters());

    return clone;
  }
}
