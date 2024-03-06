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

package com.here.xyz.httpconnector.util;

import com.here.xyz.httpconnector.CService;
import com.here.xyz.httpconnector.util.web.HubWebClientAsync;
import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;

public class Async {
  public final WorkerExecutor asyncWorkers;

  public Async(int workerPoolSize) {
    //TODO: Use new JobService class instead of CService (once existing)
    asyncWorkers = CService.vertx.createSharedWorkerExecutor(HubWebClientAsync.class.getName(), workerPoolSize);
  }

  public <R> Future<R> run(ThrowingSupplier<R> task) {
    return asyncWorkers.executeBlocking(
        promise -> {
          try {
            promise.complete(task.supply());
          }
          catch (Exception e) {
            promise.fail(e);
          }
        });
  }

  @FunctionalInterface
  public interface ThrowingSupplier<R> {
    R supply() throws Exception;
  }
}
