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
import com.here.naksha.lib.core.models.storage.ReadRequest;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import com.here.naksha.lib.core.models.storage.WriteRequest;
import com.here.naksha.lib.core.storage.IReadSession;
import com.here.naksha.lib.core.storage.IWriteSession;
import org.jetbrains.annotations.NotNull;

public class IntHandlerForStorages extends AbstractEventHandler {

  public IntHandlerForStorages(final @NotNull INaksha hub) {
    super(hub);
  }

  /**
   * The method invoked by the event-pipeline to process Storage specific read/write operations
   *
   * @param event the event to process.
   * @return the result.
   */
  @Override
  public @NotNull Result processEvent(@NotNull IEvent event) {
    final NakshaContext ctx = NakshaContext.currentContext();
    final Request<?> request = event.getRequest();
    // process request it using Naksha Admin Storage instance
    if (request instanceof ReadRequest<?> rr) {
      try (final IReadSession reader = nakshaHub().getAdminStorage().newReadSession(ctx, false)) {
        return reader.execute(rr);
      }
    } else if (request instanceof WriteRequest<?, ?> wr) {
      try (final IWriteSession writer = nakshaHub().getAdminStorage().newWriteSession(ctx, true)) {
        return writer.execute(wr);
      }
    } else {
      return notImplemented(event);
    }
  }
}
