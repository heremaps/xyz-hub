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

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.exceptions.StorageNotFoundException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventHandlerProperties;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.SuccessResult;
import com.here.naksha.lib.core.storage.IStorage;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;

public class IntHandlerForEventHandlers extends AdminFeatureEventHandler<EventHandler> {

  public IntHandlerForEventHandlers(final @NotNull INaksha hub) {
    super(hub, EventHandler.class);
  }

  @Override
  protected @NotNull Result validateFeature(EventHandler eventHandler) {
    return classNameError(eventHandler)
        .or(() -> defaultStorageHandlerError(eventHandler))
        .orElse(new SuccessResult());
  }

  private Optional<Result> classNameError(EventHandler eventHandler) {
    if (eventHandler.getClassName() == null || eventHandler.getClassName().isEmpty()) {
      return Optional.of(new ErrorResult(XyzError.ILLEGAL_ARGUMENT, "Mandatory parameter className missing!"));
    }
    return Optional.empty();
  }

  private Optional<ErrorResult> defaultStorageHandlerError(EventHandler eventHandler) {
    if (eventHandler.getClassName().equals(DefaultStorageHandler.class.getName())) {
      final Object storageId = eventHandler.getProperties().get(EventHandlerProperties.STORAGE_ID);
      if (storageId == null || storageId.toString().isEmpty()) {
        return Optional.of(new ErrorResult(
            XyzError.ILLEGAL_ARGUMENT,
            "Mandatory properties parameter %s missing!".formatted(EventHandlerProperties.STORAGE_ID))
        );
      } else {
        return invalidStorageError(storageId.toString());
      }
    }
    return Optional.empty();
  }

  private Optional<ErrorResult> invalidStorageError(String storageId) {
    try {
      nakshaHub.getStorageById(storageId);
      return Optional.empty();
    } catch (Exception e) {
      Throwable cause = e.getCause();
      if (cause instanceof StorageNotFoundException snfe) {
        return Optional.of(snfe.toErrorResult());
      }
      throw e;
    }
  }
}
