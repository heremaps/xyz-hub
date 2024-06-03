/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.app.service.http.apis;

import static com.here.naksha.app.service.http.tasks.EventHandlerApiTask.EventHandlerApiReqType.*;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.tasks.EventHandlerApiTask;
import com.here.naksha.app.service.http.tasks.EventHandlerApiTask.EventHandlerApiReqType;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHandlerApi extends Api {

  private static final Logger logger = LoggerFactory.getLogger(EventHandlerApi.class);

  public EventHandlerApi(@NotNull NakshaHttpVerticle verticle) {
    super(verticle);
  }

  @Override
  public void addOperations(@NotNull RouterBuilder rb) {
    rb.operation("postHandler").handler(this::createEventHandler);
    rb.operation("getHandlers").handler(this::getEventHandlers);
    rb.operation("getHandlerById").handler(this::getEventHandlerById);
    rb.operation("updateHandler").handler(this::updateEventHandler);
    rb.operation("deleteHandler").handler(this::deleteEventHandler);
  }

  @Override
  public void addManualRoutes(@NotNull Router router) {}

  private void createEventHandler(final @NotNull RoutingContext routingContext) {
    startHandlerApiTask(CREATE_HANDLER, routingContext);
  }

  private void getEventHandlers(final @NotNull RoutingContext routingContext) {
    startHandlerApiTask(GET_ALL_HANDLERS, routingContext);
  }

  private void getEventHandlerById(final @NotNull RoutingContext routingContext) {
    startHandlerApiTask(GET_HANDLER_BY_ID, routingContext);
  }

  private void updateEventHandler(RoutingContext routingContext) {
    startHandlerApiTask(UPDATE_HANDLER, routingContext);
  }

  private void deleteEventHandler(RoutingContext routingContext) {
    startHandlerApiTask(DELETE_HANDLER, routingContext);
  }

  private void startHandlerApiTask(EventHandlerApiReqType reqType, RoutingContext routingContext) {
    new EventHandlerApiTask<>(
            reqType, verticle, naksha(), routingContext, verticle.createNakshaContext(routingContext))
        .start();
  }
}
