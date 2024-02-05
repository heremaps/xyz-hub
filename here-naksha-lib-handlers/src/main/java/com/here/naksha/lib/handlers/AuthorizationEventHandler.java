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
package com.here.naksha.lib.handlers;

import static com.here.naksha.lib.handlers.AbstractEventHandler.EventProcessingStrategy.SEND_UPSTREAM_WITHOUT_PROCESSING;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.storage.Result;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class AuthorizationEventHandler extends AbstractEventHandler {

  protected @Nullable Space space;
  protected @Nullable List<EventHandler> eventHandlers;

  public AuthorizationEventHandler(final @NotNull INaksha hub) {
    super(hub);
  }

  public AuthorizationEventHandler(
      final @NotNull INaksha hub, final @NotNull Space space, final @NotNull List<EventHandler> eventHandlers) {
    super(hub);
    this.space = space;
    this.eventHandlers = eventHandlers;
  }

  @Override
  protected EventProcessingStrategy processingStrategyFor(IEvent event) {
    return SEND_UPSTREAM_WITHOUT_PROCESSING;
  }

  @Override
  protected @NotNull Result process(@NotNull IEvent event) {
    // TODO : Apply authorization logic here (for now requests will be sent upstream - see EventProcessingStrategy)
    return notImplemented(event);
  }
}
