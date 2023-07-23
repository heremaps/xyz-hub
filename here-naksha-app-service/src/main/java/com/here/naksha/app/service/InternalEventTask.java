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
package com.here.naksha.app.service;

import com.here.naksha.app.service.http.HttpResponseType;
import com.here.naksha.lib.core.AbstractEventTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.EventFeature;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * A simple implementation to send events to any event-feature.
 */
public class InternalEventTask extends AbstractEventTask<Event> {

  /**
   * Creates a new even-task.
   *
   * @param naksha The reference to the Naksha host.
   */
  protected InternalEventTask(@NotNull INaksha naksha, @NotNull Event event, @NotNull EventFeature eventFeature) {
    super(naksha);
    setEvent(event);
    this.eventFeature = eventFeature;
  }

  protected @NotNull EventFeature eventFeature;

  @Override
  protected @NotNull XyzResponse errorResponse(@NotNull Throwable throwable) {
    return naksha().toErrorResponse(throwable);
  }

  @SuppressWarnings("unchecked")
  @Override
  public @NotNull List<HttpResponseType> responseTypes() {
    return (List<HttpResponseType>) super.responseTypes();
  }

  @Override
  protected void init() {
    pipeline().addEventHandler(eventFeature);
  }

  @Override
  protected @NotNull XyzResponse execute() {
    return pipeline().sendEvent(getEvent());
  }
}
