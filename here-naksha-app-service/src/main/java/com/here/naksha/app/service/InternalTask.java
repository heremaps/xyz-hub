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

import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;

/**
 * A simple implementation to execute arbitrary tasks.
 *
 * @param <RESPONSE> The response-type.
 */
public class InternalTask<RESPONSE> extends AbstractTask<RESPONSE, InternalTask<RESPONSE>> {

  /**
   * Creates a new task.
   *
   * @param naksha   The reference to the Naksha host.
   * @param supplier The supplier of the response.
   */
  protected InternalTask(@NotNull INaksha naksha, @NotNull Supplier<RESPONSE> supplier) {
    super(naksha, new NakshaContext());
    this.supplier = supplier;
  }

  private final @NotNull Supplier<RESPONSE> supplier;

  @Override
  protected void init() {}

  @Override
  protected @NotNull RESPONSE execute() {
    return supplier.get();
  }
}
