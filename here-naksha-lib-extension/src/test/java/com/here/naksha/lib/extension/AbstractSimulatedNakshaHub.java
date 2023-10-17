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
package com.here.naksha.lib.extension;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.EventFeature;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.storage.IStorage;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public abstract class AbstractSimulatedNakshaHub extends Thread implements INaksha {

  XyzResponse response;
  Exception exception;

  @Override
  public IStorage getAdminStorage() {
    // TODO : Add logic
    return null;
  }

  @Override
  public IStorage getSpaceStorage() {
    // TODO : Add logic
    return null;
  }

  @Override
  public abstract void run();

  @Override
  public @NotNull ErrorResponse toErrorResponse(@NotNull Throwable throwable) {
    return new ErrorResponse(throwable, null);
  }

  @Override
  public @NotNull <RESPONSE> Future<@Nullable RESPONSE> executeTask(@NotNull Supplier<RESPONSE> execute) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull Future<@NotNull XyzResponse> executeEvent(
      @NotNull Event event, @NotNull EventFeature eventFeature) {
    throw new UnsupportedOperationException();
  }

  @Override
  public @NotNull IStorage storage() {
    throw new UnsupportedOperationException("adminStorage");
  }
}
