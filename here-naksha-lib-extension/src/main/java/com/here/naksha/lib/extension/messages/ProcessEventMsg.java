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
package com.here.naksha.lib.extension.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.NakshaVersion;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.payload.Event;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * Send from Naksha, when the extension should process an event.
 */
@AvailableSince(NakshaVersion.v2_0_3)
@JsonTypeName(value = "naksha.ext.rpc.v1.processEvent")
public class ProcessEventMsg extends ExtensionMessage {

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String CONNECTOR = "connector";

  @AvailableSince(NakshaVersion.v2_0_3)
  public static final String EVENT = "event";

  @JsonCreator
  @AvailableSince(NakshaVersion.v2_0_3)
  public ProcessEventMsg(
      @JsonProperty(CONNECTOR) @NotNull Connector connector, @JsonProperty(EVENT) @NotNull Event event) {
    this.connector = connector;
    this.event = event;
  }

  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonProperty(CONNECTOR)
  public final @NotNull Connector connector;

  @AvailableSince(NakshaVersion.v2_0_3)
  @JsonProperty(EVENT)
  public final @NotNull Event event;
}
