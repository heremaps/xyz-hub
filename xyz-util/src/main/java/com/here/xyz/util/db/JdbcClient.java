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

package com.here.xyz.util.db;

import com.here.xyz.util.db.datasource.DataSourceProvider;
import com.mchange.v2.resourcepool.TimeoutException;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.sql.SQLException;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;
import org.apache.commons.dbutils.ResultSetHandler;

public class JdbcClient implements AutoCloseable {
  private static final ExecutorService sharedExec = Executors.newCachedThreadPool();

  private DataSourceProvider dataSourceProvider;
  private ExecutorService exec = sharedExec;
  private boolean queueing;
  private Queue<QueuedTask<?>> taskQueue;

  public JdbcClient(DataSourceProvider dataSourceProvider) {
    this.dataSourceProvider = dataSourceProvider;
  }

  public JdbcClient(DataSourceProvider dataSourceProvider, ExecutorService exec) {
    this(dataSourceProvider);
    this.exec = exec;
  }

  public Future<Void> run(SQLQuery query) {
    return run(query, false);
  }

  public <R> Future<R> run(SQLQuery query, ResultSetHandler<R> handler) {
    return run(query, handler, false);
  }

  public Future<Void> run(SQLQuery query, boolean useReplica) {
    return run(query, rs -> null, useReplica);
  }

  public <R> Future<R> run(SQLQuery query, ResultSetHandler<R> handler, boolean useReplica) {
    return runTask(() -> query.run(dataSourceProvider, handler, useReplica));
  }

  public Future<Integer> write(SQLQuery query) {
    return runTask(() -> query.write(dataSourceProvider));
  }

  private <R> Future<R> runTask(ThrowingSupplier<R> task) {
    return isQueueing() ? runTaskWithQueueing(task).onComplete(v -> checkQueue()) : runTaskWithoutQueueing(task);
  }

  private <R> Future<R> runTaskWithoutQueueing(ThrowingSupplier<R> task) {
    return Future.fromCompletionStage(CompletableFuture.supplyAsync(task, exec))
        .recover(t -> Future.failedFuture(unpackExcecutionException(t)));
  }

  private static Throwable unpackExcecutionException(Throwable t) {
    return t instanceof CompletionException
        ? unpackExcecutionException(t.getCause())
        : t instanceof QueryExecutionException ? t.getCause() : t;
  }

  @Deprecated
  private <R> Future<R> runTaskWithQueueing(ThrowingSupplier<R> task) {
    if (taskQueue.isEmpty())
      return tryRunTaskAndEnqueueOnTimeout(task);

    //If the queue contains tasks already, add the task to the end of the queue
    return enqueue(task);
  }

  @Deprecated
  private <R> Future<R> enqueue(ThrowingSupplier<R> task) {
    QueuedTask<R> queuedTask = new QueuedTask<>(task);
    taskQueue.add(queuedTask);
    return queuedTask.promise.future();
  }

  @Deprecated
  private <R> Future<R> tryRunTaskAndEnqueueOnTimeout(ThrowingSupplier<R> task) {
    return runTaskWithoutQueueing(task)
        .recover(t -> t instanceof SQLException && t.getCause() instanceof TimeoutException ? enqueue(task) : Future.failedFuture(t));
  }

  @Deprecated
  private <R> void checkQueue() {
    QueuedTask<R> queuedTask = (QueuedTask<R>) taskQueue.poll();
    if (queuedTask == null)
      return;

    runTaskWithoutQueueing(queuedTask.task)
        .onComplete(result -> {
          if (result.failed())
            queuedTask.promise.fail(result.cause());
          else
            queuedTask.promise.complete(result.result());
        });
  }

  public boolean hasReader() {
    return dataSourceProvider.hasReader();
  }

  public DataSourceProvider getDataSourceProvider() {
    return dataSourceProvider;
  }

  @Override
  public void close() throws Exception {
    dataSourceProvider.close();
  }

  @Deprecated
  public boolean isQueueing() {
    return queueing;
  }

  @Deprecated
  public void setQueueing(boolean queueing) {
    if (queueing && taskQueue == null)
      taskQueue = new ConcurrentLinkedQueue<>();
    else if (taskQueue != null && !taskQueue.isEmpty())
      throw new IllegalStateException("Queueing can not be deactivated as the queue is not empty.");
    else
      taskQueue = null;
    this.queueing = queueing;
  }

  @Deprecated
  public JdbcClient withQueueing(boolean queueing) {
    setQueueing(queueing);
    return this;
  }

  @FunctionalInterface
  public interface ThrowingSupplier<R> extends Supplier<R> {
    R supply() throws Exception;

    @Override
    default R get() {
      try {
        return supply();
      }
      catch (Exception e) {
        throw new QueryExecutionException(e);
      }
    }
  }

  @Deprecated
  private class QueuedTask<R> {
    private final Promise<R> promise = Promise.promise();
    private final ThrowingSupplier<R> task;
    private QueuedTask(ThrowingSupplier task) {
      this.task = task;
    }
  }

  public static class QueryExecutionException extends RuntimeException {
    public QueryExecutionException(Exception cause) {
      super(cause);
    }

    public QueryExecutionException(String message, Exception cause) {
      super(message, cause);
    }
  }
}
