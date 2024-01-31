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

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.exceptions.StorageNotFoundException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.handlers.DefaultStorageHandler;
import com.here.naksha.lib.handlers.DefaultStorageHandlerProperties;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

public class IntHandlerForEventHandlers extends AdminFeatureEventHandler<EventHandler> {

  public IntHandlerForEventHandlers(final @NotNull INaksha hub) {
    super(hub, EventHandler.class);
  }

  @Override
  protected @NotNull Result validateFeature(XyzFeatureCodec codec) {
    Result basicValidation = super.validateFeature(codec);
    if (basicValidation instanceof ErrorResult) {
      return basicValidation;
    }
    final EventHandler eventHandler = (EventHandler) codec.getFeature();
    Result pluginValidation = PluginPropertiesValidator.pluginValidation(eventHandler);
    if (pluginValidation instanceof ErrorResult) {
      return pluginValidation;
    }
    return defaultStorageValidation(eventHandler);
  }

  private Result defaultStorageValidation(EventHandler eventHandler) {
    if (isDefaultStorageHandler(eventHandler)) {
      return storageValidationError(eventHandler);
    }
    return new SuccessResult();
  }

  private boolean isDefaultStorageHandler(@NotNull EventHandler eventHandler) {
    return DefaultStorageHandler.class.getName().equals(eventHandler.getClassName());
  }

  private @NotNull Result storageValidationError(@NotNull EventHandler eventHandler) {
    Object storageIdProp = eventHandler.getProperties().get(DefaultStorageHandlerProperties.STORAGE_ID);
    if (storageIdProp == null) {
      return new ErrorResult(
          XyzError.ILLEGAL_ARGUMENT,
          "Mandatory properties parameter %s missing!".formatted(DefaultStorageHandlerProperties.STORAGE_ID));
    }
    String storageId = storageIdProp.toString();
    if (StringUtils.isBlank(storageId)) {
      return new ErrorResult(
          XyzError.ILLEGAL_ARGUMENT,
          "Mandatory parameter %s can't be empty/blank!"
              .formatted(DefaultStorageHandlerProperties.STORAGE_ID));
    }
    return storageExistenceValidation(storageId);
  }

  /**
   * Verifies whether supplied storageId points at existing storage
   *
   * @param storageId
   * @return ErrorResult or null if storage exists
   */
  private @NotNull Result storageExistenceValidation(@NotNull String storageId) {
    try {
      nakshaHub.getStorageById(storageId);
    } catch (StorageNotFoundException snfe) {
      return snfe.toErrorResult();
    }
    return new SuccessResult();
  }
}
