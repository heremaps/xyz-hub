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

package com.here.xyz.util;

import com.here.xyz.util.service.Core;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;
import java.util.concurrent.atomic.AtomicReference;

public class Async {
  public final AtomicReference<WorkerExecutor> asyncWorkers = new AtomicReference<>();
  private String name;
  private final int workerPoolSize;

  public Async(int workerPoolSize, Class<?> callerClass) {
    name = callerClass.getName();
    this.workerPoolSize = workerPoolSize;
  }

  private WorkerExecutor exec() {
    if (asyncWorkers.get() == null) {
      WorkerExecutor workers = Core.vertx.createSharedWorkerExecutor(name, workerPoolSize);
      if (!asyncWorkers.compareAndSet(null, workers))
        //Some other thread initialized the workers already
        workers.close();
    }
    return asyncWorkers.get();
  }

  public <R> Future<R> run(ThrowingSupplier<R> task) {
    return exec().executeBlocking(
        promise -> {
          try {
            promise.complete(task.supply());
          }
          catch (Exception e) {
            promise.fail(e);
          }
        }, false);
  }

  @FunctionalInterface
  public interface ThrowingSupplier<R> {
    R supply() throws Exception;
  }
}
