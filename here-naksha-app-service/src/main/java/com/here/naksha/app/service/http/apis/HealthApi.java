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

import static com.here.naksha.lib.core.util.MIMEType.APPLICATION_JSON;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;

import com.here.naksha.app.service.http.NakshaHttpVerticle;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.jetbrains.annotations.NotNull;

public class HealthApi extends Api {

  public HealthApi(@NotNull NakshaHttpVerticle verticle) {
    super(verticle);
  }

  @Override
  public void addOperations(@NotNull RouterBuilder rb) {
    rb.operation("getHealthStatus").handler(this::onHealthStatus);
  }

  @Override
  public void addManualRoutes(@NotNull Router router) {
    router.route(HttpMethod.GET, "/").handler(this::onHealthStatus);
    router.route(HttpMethod.GET, "/hub").handler(this::onHealthStatus);
    // Legacy.
    // router.route(HttpMethod.GET, "/hub/health-status").handler(this::onHealthStatus);
  }

  private void onHealthStatus(final RoutingContext context) {
    context.response()
        .setStatusCode(OK.code())
        .putHeader(HttpHeaders.CONTENT_TYPE, APPLICATION_JSON)
        .end("{\"status\":\"OK\"}");
  }
}
