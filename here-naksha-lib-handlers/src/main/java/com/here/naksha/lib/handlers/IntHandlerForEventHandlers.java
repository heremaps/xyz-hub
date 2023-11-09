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

import com.here.naksha.lib.core.IEvent;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.naksha.EventHandlerProperties;
import com.here.naksha.lib.core.models.storage.*;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class IntHandlerForEventHandlers extends AbstractEventHandler {

  public IntHandlerForEventHandlers(final @NotNull INaksha hub) {
    super(hub);
  }

  /**
   * The method invoked by the event-pipeline to process EventHandler specific read/write operations
   *
   * @param event the event to process.
   * @return the result.
   */
  @Override
  public @NotNull Result processEvent(@NotNull IEvent event) {
    final NakshaContext ctx = NakshaContext.currentContext();
    final Request<?> request = event.getRequest();
    // process request using Naksha Admin Storage instance
    if (request instanceof ReadRequest<?> rr) {
      try (final IReadSession reader = nakshaHub().getAdminStorage().newReadSession(ctx, false)) {
        return reader.execute(rr);
      }
    } else if (request instanceof WriteRequest<?, ?> wr) {
      // validate the request before persisting
      final Result valResult = validateWriteRequest(wr);
      if (valResult instanceof ErrorResult er) return er;
      // persist in storage
      try (final IWriteSession writer = nakshaHub().getAdminStorage().newWriteSession(ctx, true)) {
        final Result result = writer.execute(wr);
        if (result instanceof SuccessResult) writer.commit();
        return result;
      }
    } else {
      return notImplemented(event);
    }
  }

  private @NotNull Result validateWriteRequest(final @NotNull WriteRequest<?, ?> wr) {
    Result result = null;
    for (final WriteOp<?> wOp : wr.queries) {
      final EventHandler eventHandler = (EventHandler) wOp.feature;
      // Common Plugin specific validations
      result = validateWritePluginRequest(eventHandler);
      if (result instanceof ErrorResult) return result;
      // TODO : handler specific validations in future, as needed
      if (eventHandler.getClassName().equals(DefaultStorageHandler.class.getName())) {
        final Object storageId = eventHandler.getProperties().get(EventHandlerProperties.STORAGE_ID);
        if (storageId == null || storageId.toString().isEmpty())
          return new ErrorResult(XyzError.ILLEGAL_ARGUMENT, "Mandatory parameter properties.storageId missing!");
        // TODO MCPODS-6574 check if storageId is valid
      }
    }
    return new SuccessResult();
  }
}
