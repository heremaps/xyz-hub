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

import static com.here.naksha.lib.core.NakshaAdminCollection.EVENT_HANDLERS;
import static com.here.naksha.lib.core.util.storage.ResultHelper.readFeaturesFromResult;
import static com.here.naksha.lib.handlers.internal.PluginPropertiesValidator.pluginValidation;

import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.exceptions.NoCursor;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.geojson.implementation.XyzFeature;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventHandlerProperties;
import com.here.naksha.lib.core.models.naksha.Storage;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.util.storage.RequestHelper;
import java.util.List;
import java.util.NoSuchElementException;
import org.jetbrains.annotations.NotNull;

public class IntHandlerForStorages extends AdminFeatureEventHandler<Storage> {

  public IntHandlerForStorages(final @NotNull INaksha hub) {
    super(hub, Storage.class);
  }

  @Override
  protected @NotNull Result validateFeature(XyzFeatureCodec codec) {
    final EWriteOp operation = EWriteOp.get(codec.getOp());
    if (operation.equals(EWriteOp.DELETE)) {
      // For DELETE, only the feature ID is needed, other JSON properties are irrelevant
      return noActiveHandlerValidation(codec);
    }
    // For non-DELETE write request
    Result basicValidation = super.validateFeature(codec);
    if (basicValidation instanceof ErrorResult) {
      return basicValidation;
    }
    final Storage storage = (Storage) codec.getFeature();
    return pluginValidation(storage);
  }

  private Result noActiveHandlerValidation(XyzFeatureCodec codec) {
    // Search for active event handlers still using this storage
    String storageId = codec.getId();
    if (storageId == null) {
      if (codec.getFeature() == null) {
        return new ErrorResult(XyzError.ILLEGAL_ARGUMENT, "No storage ID supplied.");
      }
      storageId = codec.getFeature().getId();
    }
    // Scan through all handlers with JSON property "properties.storageId" = <storage-id-to-be-deleted>
    final PRef pRef =
        RequestHelper.pRefFromPropPath(new String[] {XyzFeature.PROPERTIES, EventHandlerProperties.STORAGE_ID});
    final POp activeHandlersPOp = POp.eq(pRef, storageId);
    final ReadFeatures readActiveHandlersRequest =
        new ReadFeatures(EVENT_HANDLERS).withPropertyOp(activeHandlersPOp);
    try (final IReadSession readSession =
        nakshaHub().getAdminStorage().newReadSession(NakshaContext.currentContext(), false)) {
      final Result readResult = readSession.execute(readActiveHandlersRequest);
      if (!(readResult instanceof SuccessResult)) {
        return readResult;
      }
      final List<EventHandler> eventHandlers;
      try {
        eventHandlers = readFeaturesFromResult(readResult, EventHandler.class);
      } catch (NoCursor | NoSuchElementException emptyException) {
        // No active handler using the storage, proceed with deleting the storage
        return new SuccessResult();
      } finally {
        readResult.close();
      }
      final List<String> handlerIds =
          eventHandlers.stream().map(XyzFeature::getId).toList();
      return new ErrorResult(
          XyzError.CONFLICT, "The storage is still in use by these event handlers: " + handlerIds);
    }
  }
}
