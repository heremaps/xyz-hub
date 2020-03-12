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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Map;

/**
 * Ask the xyz storage connector to return the current HEAD state of the objects with the given identifiers. Additionally the query may ask
 * for a specific state of the feature, if the state hash is provided as value in the idsMap property. In that case and when the storage
 * provider does have an object history, it should return the feature in the HEAD and in the requested state. If both, the HEAD state and
 * the requested state, are the same, then only this state should be returned.
 *
 * <b>Note/<b>: A specific state is requested when a merge operation may be needed. This merging of concurrent changes is automatically
 * done by the API gateway, if the xyz connector does support returning objects in history state.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "LoadFeaturesEvent")
public final class LoadFeaturesEvent extends Event<LoadFeaturesEvent> {

  public LoadFeaturesEvent() {
    setPreferPrimaryDataSource(true);
  }

  @JsonInclude(Include.ALWAYS)
  private Map<String, String> idsMap;
  private Boolean enableHistory;

  /**
   * Returns the IDs map, that is a map where the key contains the unique ID of the feature to be loaded. The value is the state hash or
   * null, if the HEAD state is requested.
   *
   * If the current HEAD state does not match the provided hash, both states, the one requested plus the current HEAD state should be
   * returned. If the service does not have a history it may omit to return the old state and only return the HEAD state.
   *
   * If an object does exist in the requested state, but it does no longer have a HEAD state (it was deleted), then no state must be
   * returned.
   *
   * @return the IDs map.
   */
  @SuppressWarnings("unused")
  public Map<String, String> getIdsMap() {
    return this.idsMap;
  }

  public void setIdsMap(Map<String, String> idsMap) {
    this.idsMap = idsMap;
  }

  public LoadFeaturesEvent withIdsMap(Map<String, String> idsMap) {
    setIdsMap(idsMap);
    return this;
  }

  /**
   * Returns true if the hash should be maintained.
   *
   * @return true if the hash should be maintained, false otherwise.
   */
  @SuppressWarnings("unused")
  public Boolean getEnableHistory() {
    return this.enableHistory;
  }

  /**
   * Sets the enabler for uuid.
   *
   * @param enableHistory if true, then set an uuid for each feature state
   */
  @SuppressWarnings("WeakerAccess")
  public void setEnableHistory(Boolean enableHistory) {
    this.enableHistory = enableHistory;
  }

  @SuppressWarnings("unused")
  public LoadFeaturesEvent withEnableUUID(Boolean enableHistory) {
    setEnableHistory(enableHistory);
    return this;
  }
}
