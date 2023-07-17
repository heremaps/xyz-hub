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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All configurations that represent a component with an event-pipeline must extend this class. A connector component is a component, that
 * acts on events send through an event-pipeline into which connectors added.
 */
@SuppressWarnings("unused")
@AvailableSince(NakshaVersion.v2_0_0)
public abstract class ConnectorComponent extends XyzFeature {

  @AvailableSince(NakshaVersion.v2_0_0)
  public static final String EVENT_HANDLERS = "eventHandlers";

  /**
   * Create a new empty pipeline.
   *
   * @param id           The identifier of this component.
   * @param connectorIds The IDs of all connectors to add to the event-pipeline.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  public ConnectorComponent(@NotNull String id, @NotNull List<@NotNull String> connectorIds) {
    super(id);
    this.connectorIds = connectorIds;
  }

  /**
   * Create a new empty pipeline.
   *
   * @param id            the ID.
   * @param eventHandlers the list of event-handler identifier of the event handlers that form the event-pipeline.
   * @param packages      the packages this feature is part of.
   */
  @AvailableSince(NakshaVersion.v2_0_0)
  public ConnectorComponent(
      @NotNull String id,
      @NotNull List<@NotNull String> eventHandlers,
      @Nullable List<@NotNull String> packages) {
    super(id);
    setConnectorIds(eventHandlers);
    setPackages(packages);
  }

  /**
   * The list of event-handler identifiers to be added to the event pipeline, in order.
   */
  @AvailableSince(NakshaVersion.v2_0_6)
  @JsonProperty(EVENT_HANDLERS)
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
   * Replace the identifiers of the connectors.
   *
   * @param connectorIds The identifiers of all connectors.
   */
  @AvailableSince(NakshaVersion.v2_0_6)
  public void setConnectorIds(@NotNull String... connectorIds) {
    this.connectorIds = new ArrayList<>(connectorIds.length);
    Collections.addAll(this.connectorIds, connectorIds);
  }
}
