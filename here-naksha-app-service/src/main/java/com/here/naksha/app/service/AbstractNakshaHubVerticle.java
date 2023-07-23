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

import com.here.naksha.lib.core.INakshaBound;
import io.vertx.core.AbstractVerticle;
import org.jetbrains.annotations.NotNull;

/**
 * All verticles used by the Naksha-Hub must extend this one.
 */
public class AbstractNakshaHubVerticle extends AbstractVerticle implements INakshaBound {

  /**
   * Creates a new Naksha-Hub verticle.
   *
   * @param naksha The naksha-hub.
   * @param index  The index in the {@link NakshaHub#verticles} array of the Naksha-Hub.
   */
  protected AbstractNakshaHubVerticle(@NotNull NakshaHub naksha, int index) {
    this.naksha = naksha;
    this.index = index;
  }

  /**
   * The Naksha-Hub to which the verticle belongs.
   */
  final @NotNull NakshaHub naksha;

  @Override
  public @NotNull NakshaHub naksha() {
    return naksha;
  }

  /**
   * The index in the {@link NakshaHub#verticles} array.
   */
  final int index;
}
