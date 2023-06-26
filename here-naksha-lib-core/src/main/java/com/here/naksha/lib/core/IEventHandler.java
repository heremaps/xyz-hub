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
package com.here.naksha.lib.core;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import org.jetbrains.annotations.NotNull;

/**
 * The default interface that must be implemented by all event handler. A handler is code that
 * performs actions with an event bound to an event context provided by the host application. Note
 * that the host will create a new event pipeline for every new event, add all necessary handlers to
 * it and then send the event through the pipeline. The pipeline itself provides the {@link
 * IEventContext} and invokes all handlers in order. Every handler can either consume the event or
 * {@link IEventContext#sendUpstream(Event) send it upstream}.
 */
@FunctionalInterface
public interface IEventHandler {

  /**
   * The method invoked by the XYZ-Hub directly (embedded) or indirectly, when running in an HTTP
   * vertx or as AWS lambda.
   *
   * @param eventContext the event context to process.
   * @return the response to send.
   * @throws XyzErrorException if any error occurred.
   */
  @NotNull
  XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException;
}
