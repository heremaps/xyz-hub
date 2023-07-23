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
package com.here.naksha.app.service.http;

import com.here.naksha.app.service.InternalEventTask;
import com.here.naksha.lib.core.models.EventFeature;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * When Naksha has generated an event for an {@link EventFeature}, then it should send the event to the event-feature, as logical operation.
 * Actually, this special task should be used, when an event was generated from an HTTP request and requires to send back an HTTP response.
 */
public class NakshaHttpEventTask extends InternalEventTask {

  /**
   * Creates a new task to send the given event to the event-pipeline described by the given even-feature.
   * @param routingContext The routing context of the HTTP request that was used to generate the event.
   * @param httpVerticle The HTTP verticle that received the HTTP request.
   * @param event The event
   * @param eventFeature The feature-event to which to send the event to.
   */
  public NakshaHttpEventTask(
      @NotNull RoutingContext routingContext,
      @NotNull NakshaHttpVerticle httpVerticle,
      @NotNull Event event,
      @NotNull EventFeature eventFeature) {
    super(httpVerticle.naksha(), event, eventFeature);
    this.routingContext = routingContext;
    this.httpVerticle = httpVerticle;
    addListener(this::sendResponse);
  }

  private void sendResponse(@NotNull XyzResponse response) {
    // TODO: Review the request and see if he accepts different response types and which is the optimal in this
    // case.
    //
    // final HttpServerRequest request = routingContext.request();
    //
    final List<HttpResponseType> responseTypes = responseTypes();
    final HttpResponseType responseType = responseTypes.size() > 0 ? responseTypes.get(0) : null;
    httpVerticle.sendXyzResponse(routingContext, responseType, response);
  }

  /**
   * The routing context that created this task.
   */
  final @NotNull RoutingContext routingContext;

  /**
   * The HTTP verticle that created this task.
   */
  final @NotNull NakshaHttpVerticle httpVerticle;
}
