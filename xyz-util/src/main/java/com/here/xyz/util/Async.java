/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Async {
  private static final Logger logger = LogManager.getLogger();
  private final ExecutorService exec;
  private final ScheduledExecutorService timoutCheckerExec;

  private String name;

  //TODO: Monitor long running threads
  //TODO: Monitor thread pool utilization and throw warning if usage is over 90%

  public Async(int workerPoolSize, Class<?> callerClass) {
    name = callerClass.getName();
    exec = Executors.newFixedThreadPool(workerPoolSize, new ThreadFactoryBuilder().setNameFormat("async-" + name + "-%d").build());
    timoutCheckerExec = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("async-timer-" + name + "-%d")
        .build());
  }

  public <R> io.vertx.core.Future<R> run(Callable<R> task) {
    return run(task, -1, null);
  }

  public <R> io.vertx.core.Future<R> run(Callable<R> task, long timeout, TimeUnit unit) {
    CompletableFuture<R> promise = new CompletableFuture<>();
    Future future = exec.submit(() -> {
      try {
        promise.complete(task.call());
      }
      catch (Exception e) {
        promise.completeExceptionally(e);
      }
      return null;
    });

    if (timeout >= 0) {
      timoutCheckerExec.schedule(() -> {
        if (!future.isDone()) {
          if (!promise.isDone()) {
            promise.completeExceptionally(new TimeoutException("Timeout of task in " + name + " after " + timeout + " "
                + unit.toString().toLowerCase()));
          }
          future.cancel(true);
        }
      }, timeout, unit);
    }

    return io.vertx.core.Future.fromCompletionStage(promise);
  }
}
