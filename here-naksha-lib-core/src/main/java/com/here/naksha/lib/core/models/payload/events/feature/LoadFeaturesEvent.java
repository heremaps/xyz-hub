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
package com.here.naksha.lib.core.models.payload.events.feature;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.models.geojson.implementation.XyzAction;
import com.here.naksha.lib.core.models.payload.events.FeatureEvent;
import java.util.HashMap;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Ask the storage to return the current head and base state of the features with the given
 * identifiers. In that case and when the storage provider does have a history, it should return the
 * feature in the head and in the base state. If both, the head, and the base state, are the same,
 * then only this state should be returned.
 *
 * <p><b>Note</b>: A base state is for example requested, when a merge operation may be needed.
 *
 * @since 0.1.0
 */
@JsonTypeName(value = "LoadFeaturesEvent")
public final class LoadFeaturesEvent extends FeatureEvent {

  public LoadFeaturesEvent() {
    setPreferPrimaryDataSource(true);
    idsMap = new HashMap<>();
  }

  /**
   * The IDs map, that is a map where the key contains the unique ID of the feature to be loaded.
   * The value is the base uuid or {@code null}, if only the head state requested.
   *
   * <p>If the current head state does not match the provided uuid, so head differs from base, both
   * states should be returned. If the service does not have a history it may omit to return the
   * base state and only return the head state.
   *
   * @since 0.1.0
   */
  @JsonInclude(Include.NON_EMPTY)
  private @NotNull Map<@NotNull String, @Nullable String> idsMap;

  /**
   * If a feature does exist in the history, but is in the state deleted ({@link XyzAction#DELETE}),
   * this state should only be returned, if this property is {@code true}.
   *
   * <p>This only applies if the storage does have a history that keeps track of deleted features.
   *
   * <p>In a nutshell: {@code true} if deleted states should be returned; {@code false} otherwise.
   *
   * @since 2.0.0
   */
  @JsonInclude(Include.NON_DEFAULT)
  private boolean returnDeletedStates;

  @SuppressWarnings("unused")
  public @NotNull Map<@NotNull String, @Nullable String> getIdsMap() {
    return idsMap;
  }

  public void setIdsMap(@NotNull Map<@NotNull String, @Nullable String> idsMap) {
    this.idsMap = idsMap;
  }

  public @NotNull LoadFeaturesEvent withIdsMap(@NotNull Map<@NotNull String, @Nullable String> idsMap) {
    setIdsMap(idsMap);
    return this;
  }

  public boolean returnDeletedStates() {
    return returnDeletedStates;
  }

  public void returnDeletedStates(boolean returnDeletedStates) {
    this.returnDeletedStates = returnDeletedStates;
  }
}
