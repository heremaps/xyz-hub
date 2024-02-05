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

import static com.here.naksha.lib.core.NakshaAdminCollection.SPACES;
import static com.here.naksha.lib.core.models.naksha.EventTarget.EVENT_HANDLER_IDS;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesFromResult;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.exceptions.StorageNotFoundException;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.Space;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import com.here.naksha.lib.handlers.DefaultStorageHandler;
import com.here.naksha.lib.handlers.DefaultStorageHandlerProperties;
import java.util.List;
import java.util.NoSuchElementException;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;

public class IntHandlerForEventHandlers extends AdminFeatureEventHandler<EventHandler> {

  public IntHandlerForEventHandlers(final @NotNull INaksha hub) {
    super(hub, EventHandler.class);
  }

  @Override
  protected @NotNull Result validateFeature(XyzFeatureCodec codec) {
    final EWriteOp operation = EWriteOp.get(codec.getOp());
    if (operation.equals(EWriteOp.DELETE)) {
      // For DELETE, only the feature ID is needed, other JSON properties are irrelevant
      return noActiveSpaceValidation(codec);
    }
    // For non-DELETE write request
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

  private Result noActiveSpaceValidation(XyzFeatureCodec codec) {
    // Search for active event handlers still using this storage
    String handlerId = codec.getId();
    if (handlerId == null) {
      if (codec.getFeature() == null) {
        return new ErrorResult(XyzError.ILLEGAL_ARGUMENT, "No handler ID supplied.");
      }
      handlerId = codec.getFeature().getId();
    }
    // Scan through all spaces with JSON property "eventHandlerIds" containing the targeted handler ID
    final PRef pRef = RequestHelper.pRefFromPropPath(new String[] {EVENT_HANDLER_IDS});
    final POp activeSpacesPOp = POp.contains(pRef, handlerId);
    final ReadFeatures readActiveHandlersRequest = new ReadFeatures(SPACES).withPropertyOp(activeSpacesPOp);
    try (final IReadSession readSession =
        nakshaHub().getAdminStorage().newReadSession(NakshaContext.currentContext(), false)) {
      final Result readResult = readSession.execute(readActiveHandlersRequest);
      if (!(readResult instanceof SuccessResult)) {
        return readResult;
      }
      final List<Space> spaces;
      try {
        spaces = readFeaturesFromResult(readResult, Space.class);
      } catch (NoCursor | NoSuchElementException emptyException) {
        // No active space using the handler, proceed with deleting the handler
        return new SuccessResult();
      } finally {
        readResult.close();
      }
      final List<String> spaceIds = spaces.stream().map(XyzFeature::getId).toList();
      return new ErrorResult(XyzError.CONFLICT, "The event handler is still in use by these spaces: " + spaceIds);
    }
  }
}
