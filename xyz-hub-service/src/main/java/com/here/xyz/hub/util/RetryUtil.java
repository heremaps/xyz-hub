/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.hub.util;

import com.here.xyz.hub.Core;
import com.here.xyz.hub.Service;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.util.function.Function;

public class RetryUtil {

  /**
   * Automatically retries a task which is expected to have a chance for failure.
   *
   * @param task The task to be executed
   * @param shouldRetryAfter A function which returns a positive value if the operation should be retried after the amount
   *  of milliseconds (backoff), a negative value otherwise
   * @param maxRetries A maximum amount of retries to be performed. Setting this value to 0 no retry at all will be performed and a failure
   *  would be directly reported.
   * @param retryTimeout
   *  An amount of milliseconds after which no retry will be performed anymore. If specified to a value >0, retries will only be performed
   *  if the overall elapsed time does not exceed the specified value.
   * @param <R> The return value of the task
   * @return A future for the task which gets executed
   * @throws Throwable The potential error which happened during the execution of the task after all retries.
   */
  public static <R> TaskDelegate<R> executeWithRetry(Task<R> task, Function<Throwable, Integer> shouldRetryAfter, int maxRetries, int retryTimeout) {
    final TaskDelegate<R> td = task instanceof TaskDelegate ? (TaskDelegate<R>) task : new TaskDelegate(task);
    Future<R> f;
    try {
      f = td.execute(td);
    }
    catch (Throwable t) {
      f = Future.failedFuture(t);
    }
    td.future = f.compose(r -> Future.succeededFuture(r), t -> retry(t, td, shouldRetryAfter, maxRetries, retryTimeout));
    return td;
  }

  private static <R> Future<R> retry(Throwable err, TaskDelegate<R> task, Function<Throwable, Integer> shouldRetryAfter, int maxRetries, int retryTimeout) {
    int retryAfter = shouldRetryAfter.apply(err);
    if (retryAfter >= 0 && task.retries++ < maxRetries && Core.currentTimeMillis() - task.createdAt < retryTimeout && !task.cancelled) {
      Promise<R> p = Promise.promise();
      task.retryTimer = Service.vertx.setTimer(retryAfter, timerId -> executeWithRetry(task, shouldRetryAfter, maxRetries, retryTimeout)
          .future()
          .onSuccess(r -> p.complete(r))
          .onFailure(t -> p.fail(t)));
      return p.future();
    }
    else
      return Future.failedFuture(err);
  }

  public interface Task<R> {
    Future<R> execute(TaskDelegate<R> task) throws Throwable;
  }

  public static class TaskDelegate<R> implements Task<R> {
    private Task<R> task;
    private int retries = 0;
    private final long createdAt = Core.currentTimeMillis();
    private Future<R> future;
    private long retryTimer = -1;
    private boolean cancelled;

    private TaskDelegate(Task<R> delegate) {
      task = delegate;
    }

    @Override
    public Future<R> execute(TaskDelegate<R> task) throws Throwable {
      return this.task.execute(task);
    }

    public Future<R> future() {
      return future;
    }

    public void cancelRetries() {
      if (retryTimer >= 0)
        Service.vertx.cancelTimer(retryTimer);
      cancelled = true;
    }
  }

}
