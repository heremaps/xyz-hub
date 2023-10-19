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

import com.here.naksha.app.service.NakshaApp;
import com.here.naksha.app.service.http.NakshaHttpVerticle;
import com.here.naksha.lib.core.INakshaBound;
import com.here.naksha.lib.hub.NakshaHub;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.openapi.RouterBuilder;
import org.jetbrains.annotations.NotNull;

public abstract class Api implements INakshaBound {

  protected Api(@NotNull NakshaHttpVerticle verticle) {
    this.verticle = verticle;
  }

  protected final @NotNull NakshaHttpVerticle verticle;

  @Override
  public @NotNull NakshaHub naksha() {
    return verticle.naksha();
  }

  public @NotNull NakshaApp app() {
    return verticle.app();
  }

  public abstract void addOperations(@NotNull RouterBuilder rb);

  public abstract void addManualRoutes(@NotNull Router router);
}
