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
package com.here.xyz.hub.events;

import com.here.naksha.lib.core.models.hub.Connector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Pseudo-event to return a connector by ID.
 */
public class ModifyConnectorsEvent extends AbstractConnectorEvent {

  public ModifyConnectorsEvent() {
    this.ids = new ArrayList<>();
  }

  public @Nullable List<@NotNull Connector> insertFeatures;
  public @Nullable List<@NotNull Connector> updateFeatures;
  public @Nullable List<@NotNull Connector> upsertFeatures;
  public @Nullable Map<@NotNull String, @Nullable String> deleteFeatures;
}
