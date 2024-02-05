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
package com.here.naksha.lib.handlers.internal;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.handlers.AbstractEventHandler;
import org.jetbrains.annotations.NotNull;

public class IntHandlerForConfigs extends AbstractEventHandler {

  public IntHandlerForConfigs(final @NotNull INaksha hub) {
    super(hub);
  }

  @Override
  protected EventProcessingStrategy processingStrategyFor(IEvent event) {
    return EventProcessingStrategy.PROCESS;
  }

  @Override
  protected @NotNull Result process(@NotNull IEvent event) {
    return notImplemented(event);
  }
}
