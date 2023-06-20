/*
 * Copyright (C) 2017-2020 HERE Europe B.V.
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

package com.here.xyz.util.modify;

import static com.here.xyz.util.modify.IfExists.MERGE;
import static com.here.xyz.util.modify.IfNotExists.CREATE;

import com.here.xyz.models.payload.events.feature.LoadFeaturesEvent;
import com.here.xyz.models.geojson.implementation.Action;
import com.here.xyz.models.geojson.implementation.Feature;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A list of features that should be modified.
 */
public class FeatureModificationList<FEATURE extends Feature, ENTRY extends FeatureModificationEntry<FEATURE>> implements Iterable<ENTRY> {

  /**
   * All features by action. Filled after {@link FeatureModificationEntry#apply()} has been invoked.
   */
  private final HashMap<@NotNull Action, @NotNull List<@NotNull ENTRY>> usedActions = new HashMap<>();

  /**
   * All features, wrapped into an entry.
   */
  private final ArrayList<@NotNull ENTRY> entries = new ArrayList<>();

  /**
   * The action to take, when a feature does exist.
   */
  private @NotNull IfExists ifExistsDefault;

  /**
   * The action to take, when a feature does not exist.
   */
  private @NotNull IfNotExists ifNotExistsDefault;

  /**
   * Create an empty feature modification list
   */
  public FeatureModificationList() {
    ifExistsDefault = MERGE;
    ifNotExistsDefault = CREATE;
  }

  /**
   * Add the given feature.
   *
   * @param feature The feature to add.
   * @return this.
   */
  public @NotNull FeatureModificationList<FEATURE, ENTRY> add(@NotNull FEATURE feature) {
    // TODO: Implement me!
    return this;
  }

  /**
   * Add all given features.
   *
   * @param features The features to add.
   * @return this.
   */
  public @NotNull FeatureModificationList<FEATURE, ENTRY> addAll(@Nullable List<@NotNull FEATURE> features) {
    if (features != null) {
      for (final var feature : features) {
        add(feature);
      }
    }
    return this;
  }

  public IfExists getIfExistsDefault() {
    return ifExistsDefault;
  }

  public void setIfExistsDefault(IfExists ifExistsDefault) {
    this.ifExistsDefault = ifExistsDefault;
  }

  public IfNotExists getIfNotExistsDefault() {
    return ifNotExistsDefault;
  }

  public void setIfNotExistsDefault(IfNotExists ifNotExistsDefault) {
    this.ifNotExistsDefault = ifNotExistsDefault;
  }

  /**
   * The method will iterate the collection of modifications and generate an event to load all needed feature states to perform the
   * modification.
   *
   * @param allowUpsert {@code true} if upserts are supported, which means no state need to be loaded; {@code false} otherwise.
   * @return The event needed to load the feature states needed; {@code null} if nothing is necessary.
   */
  public @Nullable LoadFeaturesEvent createLoadFeaturesEvent(boolean allowUpsert) {
    if (entries.size() == 0) {
      return null;
    }
    final LoadFeaturesEvent event = new LoadFeaturesEvent();
    for (final @NotNull ENTRY entry : this) {
      if (!allowUpsert || !entry.isUpsert()) {
        event.getIdsMap().put(entry.input.getId(), entry.input.getProperties().getXyzNamespace().getUuid());
      }
    }
    return event.getIdsMap().size() > 0 ? event : null;
  }


  @Override
  public @NotNull Iterator<@NotNull ENTRY> iterator() {
    // TODO: Implement me!
    return null;
  }
}