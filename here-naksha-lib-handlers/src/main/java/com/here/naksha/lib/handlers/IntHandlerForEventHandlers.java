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
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class IntHandlerForEventHandlers extends AdminFeatureEventHandler<EventHandler> {

  public IntHandlerForEventHandlers(final @NotNull INaksha hub) {
    super(hub, EventHandler.class);
  }

  @Override
  protected @NotNull Result validateFeature(EventHandler eventHandler) {
    ErrorResult classNameError = classNameValidationError(eventHandler);
    if (classNameError != null) {
      return classNameError;
    }
    if (isDefaultStorageHandler(eventHandler)) {
      ErrorResult storageError = storageValidationError(eventHandler);
      if (storageError != null) {
        return storageError;
      }
    }
    return new SuccessResult();
  }

  private boolean isDefaultStorageHandler(@NotNull EventHandler eventHandler) {
    return DefaultStorageHandler.class.getName().equals(eventHandler.getClassName());
  }

  /**
   * Verifies whether event handler contains required `className` property
   *
   * @param eventHandler
   * @return ErrorResult or null if event handler is valid
   */
  private @Nullable ErrorResult classNameValidationError(EventHandler eventHandler) {
    if (eventHandler.getClassName() == null || eventHandler.getClassName().isEmpty()) {
      return new ErrorResult(XyzError.ILLEGAL_ARGUMENT, "Mandatory parameter className missing!");
    }
    return null;
  }

  /**
   * Verifies whether storageId defined for this handler is valid
   *
   * @param eventHandler
   * @return ErrorResult or null if event handler is valid
   */
  private @Nullable ErrorResult storageValidationError(@NotNull EventHandler eventHandler) {
    Object storageIdProp = eventHandler.getProperties().get(EventHandlerProperties.STORAGE_ID);
    if (storageIdProp == null) {
      return new ErrorResult(
          XyzError.ILLEGAL_ARGUMENT,
          "Mandatory properties parameter %s missing!".formatted(EventHandlerProperties.STORAGE_ID));
    }
    String storageId = storageIdProp.toString();
    if (StringUtils.isBlank(storageId)) {
      return new ErrorResult(
          XyzError.ILLEGAL_ARGUMENT,
          "Mandatory parameter %s can't be empty/blank!".formatted(EventHandlerProperties.STORAGE_ID));
    }
    return missingStorageError(storageId);
  }

  /**
   * Verifies whether supplied storageId points at existing storage
   *
   * @param storageId
   * @return ErrorResult or null if storage exists
   */
  private @Nullable ErrorResult missingStorageError(@NotNull String storageId) {
    try {
      nakshaHub.getStorageById(storageId);
    } catch (StorageNotFoundException snfe) {
      return snfe.toErrorResult();
    }
    return null;
  }
}
