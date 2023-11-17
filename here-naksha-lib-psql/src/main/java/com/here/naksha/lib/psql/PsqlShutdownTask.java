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
package com.here.naksha.lib.psql;

import com.here.naksha.lib.core.AbstractTask;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.NakshaContext;
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.storage.IStorage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PsqlShutdownTask<T> extends AbstractTask<T, PsqlShutdownTask<T>> {
  private static final Logger log = LoggerFactory.getLogger(PsqlShutdownTask.class);

  public PsqlShutdownTask(
      @NotNull PsqlStorage psqlStorage,
      @Nullable Fe1<T, IStorage> onShutdown,
      @Nullable INaksha naksha,
      @NotNull NakshaContext context) {
    //noinspection DataFlowIssue
    super(naksha, context);
    this.psqlStorage = psqlStorage;
    this.onShutdown = onShutdown;
    setInternal(true);
  }

  private final @NotNull PsqlStorage psqlStorage;
  private final @Nullable Fe1<T, IStorage> onShutdown;

  @Override
  protected void init() {
    psqlStorage.storage().close();
  }

  @Override
  protected @NotNull T execute() {
    // TODO: Wait for all sessions and the cursors to be collected.
    // TODO: Fix that we allow exceptions to be forwarded to the Future somehow!
    T result = null;
    if (onShutdown != null) {
      try {
        result = onShutdown.call(psqlStorage);
      } catch (Exception e) {
        log.atError()
            .setMessage("Unexpected error in shutdown handler, returning null")
            .setCause(e)
            .log();
      }
    }
    // TODO: We need a task extending AbstractTask and is EventTask, the
    //       AbstractTask should be able to handle exceptions the rights
    //       way, so that we can use it for internal tasks!
    return result;
  }
}
