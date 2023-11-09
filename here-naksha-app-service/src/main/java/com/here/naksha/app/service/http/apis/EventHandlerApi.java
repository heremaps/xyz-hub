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
package com.here.naksha.app.service.http.apis;

import static com.here.naksha.app.service.http.tasks.EventHandlerApiTask.EventHandlerApiReqType.CREATE_HANDLER;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.app.service.http.tasks.EventHandlerApiTask;
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
    rb.operation("createHandler").handler(this::createEventHandler);
  }

  @Override
  public void addManualRoutes(@NotNull Router router) {}

  private void createEventHandler(final @NotNull RoutingContext routingContext) {
    new EventHandlerApiTask<>(
            CREATE_HANDLER,
            verticle,
            naksha(),
            routingContext,
            verticle.createNakshaContext(routingContext))
        .start();
  }
}
