package com.here.xyz.util.db;

import io.vertx.core.Future;
import io.vertx.core.WorkerExecutor;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class AsyncRunner {

    public static <T, R> Future<R> executeAsync(CompletableFuture<T> future, Function<T, R> mapper) {
        return Future.future(promise -> future.handle((res, err) -> {
            if (err != null) {
                promise.fail(err);
            } else {
                promise.complete(mapper.apply(res));
            }
            return null;
        }));
    }

    public static <T> Future<T> executeAsync(CompletableFuture<T> future) {
        return Future.future(promise -> future.whenComplete((res, ex) -> {
            if (ex != null) {
                promise.fail(ex);
            } else {
                promise.complete(res);
            }
        }));
    }

    public static <T> Future<T> executeQueryAsync(AsyncRunner.ThrowingSupplier<T> commandExecution, WorkerExecutor workerExecutor) {
        return workerExecutor.executeBlocking(
                promise -> {
                    try {
                        promise.complete(commandExecution.supply());
                    } catch (Exception e) {
                        promise.fail(e);
                    }
                }
        );
    }

    @FunctionalInterface
    public interface ThrowingSupplier<S> {
        S supply() throws Exception;
    }
}
