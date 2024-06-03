/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.lib.core.models.naksha;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.naksha.lib.core.NakshaVersion;
import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class EventTarget<SELF extends EventTarget<SELF>> extends NakshaFeature {

  public static final String EVENT_HANDLER_IDS = "eventHandlerIds";

  /**
   * Create a new empty event target.
   *
   * @param id The ID; if {@code null}, then a random one is generated.
   */
  public EventTarget(@Nullable String id) {
    super(id);
    this.eventHandlerIds = new ArrayList<>();
  }

  /**
   * Create a new initialized event target.
   *
   * @param id The ID; if {@code null}, then a random one is generated.
   */
  public EventTarget(@Nullable String id, @Nullable List<@NotNull String> eventHandlerIds) {
    super(id);
    this.eventHandlerIds = eventHandlerIds == null ? new ArrayList<>() : eventHandlerIds;
  }

  @SuppressWarnings("unchecked")
  protected final @NotNull SELF self() {
    return (SELF) this;
  }

  public @NotNull List<@NotNull String> getEventHandlerIds() {
    return eventHandlerIds;
  }

  public @NotNull SELF addHandler(@NotNull String handlerId) {
    eventHandlerIds.add(handlerId);
    return self();
  }

  public @NotNull SELF addHandler(@NotNull EventHandler handler) {
    eventHandlerIds.add(handler.getId());
    return self();
  }

  @ApiStatus.AvailableSince(NakshaVersion.v2_0_7)
  @JsonProperty(EVENT_HANDLER_IDS)
  private final @NotNull List<@NotNull String> eventHandlerIds;
}
