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
package com.here.naksha.lib.handlers;

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.IEventHandler;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.util.StreamInfo;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractEventHandler implements IEventHandler {

  protected final @NotNull INaksha nakshaHub;

  public AbstractEventHandler(final @NotNull INaksha hub) {
    this.nakshaHub = hub;
  }

  public @NotNull INaksha nakshaHub() {
    return this.nakshaHub;
  }

  protected @NotNull Result notImplemented(@NotNull IEvent event) {
    return new ErrorResult(
        XyzError.NOT_IMPLEMENTED,
        "Event processing of " + event.getRequest().getClass().getSimpleName() + " in "
            + this.getClass().getSimpleName() + " is not supported");
  }

  protected void addStorageIdToStreamInfo(final @Nullable String storageId, final @NotNull NakshaContext context) {
    final StreamInfo streamInfo = context.getStreamInfo();
    if (streamInfo != null) {
      streamInfo.setStorageIdIfMissing(storageId);
    }
  }
}
