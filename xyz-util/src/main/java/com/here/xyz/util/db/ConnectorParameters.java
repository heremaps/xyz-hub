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

package com.here.xyz.util.db;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import java.util.Collections;
import java.util.Map;
import java.util.WeakHashMap;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorParameters {
  private String connectorId;
  private String ecps;
  private static Map<Map<String, Object>, ConnectorParameters> cache = Collections.synchronizedMap(new WeakHashMap<>());
  private boolean propertySearch = false;
  private boolean mvtSupport = false;
  private boolean autoIndexing = false;
  private boolean enableHashedSpaceId = false;
  private int onDemandIdxLimit = 4;
  private boolean readOnly;

  public ConnectorParameters() {}

  public String getConnectorId() {
    return connectorId;
  }

  public String getEcps() {
    return ecps;
  }

  public boolean isPropertySearch() {
    return propertySearch;
  }

  public boolean isMvtSupport() {
    return mvtSupport;
  }

  public boolean isAutoIndexing() {
    return autoIndexing;
  }

  public boolean isEnableHashedSpaceId() {
    return enableHashedSpaceId;
  }

  public int getOnDemandIdxLimit() {
    return onDemandIdxLimit;
  }

  public boolean isReadOnly() {
    return readOnly;
  }

  public void setReadOnly(boolean readOnly) {
    this.readOnly = readOnly;
  }

  public ConnectorParameters withReadOnly(boolean readOnly) {
    setReadOnly(readOnly);
    return this;
  }

  @Override
  public String toString() {
    return "ConnectorParameters{" +
            "propertySearch=" + propertySearch +
            ", mvtSupport=" + mvtSupport +
            ", autoIndexing=" + autoIndexing +
            ", enableHashedSpaceId=" + enableHashedSpaceId +
            ", onDemandIdxLimit=" + onDemandIdxLimit +
            ", ecps='" + ecps + '\'' +
            '}';
  }

  public static ConnectorParameters fromEvent(Event event) {
    final Map<String, Object> connectorParams = event.getConnectorParams();
    return fromMap(connectorParams);
  }

  public static ConnectorParameters fromMap(Map<String, Object> connectorParamsMap) {
    ConnectorParameters params = cache.get(connectorParamsMap);
    if (params == null) {
      params = connectorParamsMap != null ? XyzSerializable.fromMap(connectorParamsMap, ConnectorParameters.class) : new ConnectorParameters();
      cache.put(connectorParamsMap, params);
    }
    return params;
  }
}