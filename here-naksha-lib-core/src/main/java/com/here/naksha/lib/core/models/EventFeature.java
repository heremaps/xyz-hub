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
package com.here.naksha.lib.core.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.AbstractEventTask;
import com.here.naksha.lib.core.EventPipeline;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.payload.Event;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A feature that holds the configuration for an {@link EventPipeline}. Logically this is a feature able to process events. Normally these
 * features are used in combination with {@link INaksha} interface, so creating an event task via {@link INaksha#executeTask(Class)},
 * adding the event {@link AbstractEventTask#setEvent(Event)}, adding this feature to the {@link AbstractEventTask#pipeline pipeline} of the
 * task via {@link EventPipeline#addEventHandler(EventFeature)}.
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_0)
@Deprecated
public abstract class EventFeature extends XyzFeature {

  /**
   * Create a new empty pipeline.
   *
   * @param id           The identifier of this component.
   * @param connectorIds The list of connector identifier that form the event-pipeline.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  public EventFeature(@NotNull String id, @NotNull List<@NotNull String> connectorIds) {
    super(id);
    this.connectorIds = connectorIds;
  }

  /**
   * Create a new empty pipeline.
   *
   * @param id           The identifier of this component.
   * @param connectorIds The list of connector identifier that form the event-pipeline.
   * @param packages     The packages this feature is part of.
   */
  @JsonCreator
  @AvailableSince(NakshaVersion.v2_0_0)
  public EventFeature(
      @JsonProperty(ID) @NotNull String id,
      @JsonProperty(CONNECTOR_IDS) @NotNull List<@NotNull String> connectorIds,
      @JsonProperty(PACKAGES) @Nullable List<@NotNull String> packages) {
    super(id);
    setConnectorIds(connectorIds);
    setPackages(packages);
  }

  @AvailableSince(NakshaVersion.v2_0_6)
  public static final String CONNECTOR_IDS = "connectorIds";

  /**
   * The list of event-handler identifiers to be added to the event pipeline, in order.
   */
  @AvailableSince(NakshaVersion.v2_0_6)
  @JsonProperty(CONNECTOR_IDS)
  public @NotNull List<@NotNull String> connectorIds;

  /**
   * Returns the identifiers of all connectors.
   *
   * @return The identifiers of all connectors.
   */
  @AvailableSince(NakshaVersion.v2_0_6)
  @JsonIgnore
  public @NotNull List<@NotNull String> getConnectorIds() {
    return connectorIds;
  }

  /**
   * Replace the identifiers of the connectors.
   *
   * @param connectorIds The identifiers of all connectors.
   */
  @AvailableSince(NakshaVersion.v2_0_6)
  public void setConnectorIds(@NotNull List<@NotNull String> connectorIds) {
    this.connectorIds = connectorIds;
  }

  /**
   * TODO HP_QUERY : To be removed if found redundant (after confirmed with Alex)
   * This function is commented as it creates runtime conflict error -
   * com.fasterxml.jackson.databind.exc.InvalidDefinitionException: Conflicting setter definitions for property "connectorIds": com.here.naksha.lib.core.models.EventFeature#setConnectorIds([Ljava.lang.String;) vs com.here.naksha.lib.core.models.EventFeature#setConnectorIds(java.util.List)
   * /oo
   * o/
   * o Replace the identifiers of the connectors.
   * o
   * o @param connectorIds The identifiers of all connectors.
   * o/
   * @AvailableSince(NakshaVersion.v2_0_6)
   * public void setConnectorIds(@NotNull String... connectorIds) {
   * this.connectorIds = new ArrayList<>(connectorIds.length);
   * Collections.addAll(this.connectorIds, connectorIds);
   * }
   */
}
