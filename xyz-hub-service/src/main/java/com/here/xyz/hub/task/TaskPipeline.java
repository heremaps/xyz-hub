/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

package com.here.xyz.hub.task;

import static com.here.xyz.hub.rest.Context.logStream;
import static com.here.xyz.hub.rest.Context.logTime;

import com.here.xyz.events.Event;
import io.vertx.ext.web.RoutingContext;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A pipeline with functions to process a task.
 *
 * @param <EVENT> the event type.
 */
public final class TaskPipeline<EVENT extends Event, TASK extends AbstractEventTask<EVENT, TASK>> {

  private static final Logger logger = LoggerFactory.getLogger(TaskPipeline.class);
  private static final String logId = TaskPipeline.class.getSimpleName();

  private final class Step implements ICallback {

    private Step(@NotNull ITaskStep<EVENT, TASK> processor) {
      this.processor = processor;
    }

    private final @NotNull ITaskStep<EVENT, TASK> processor;

    @SuppressWarnings("NotNullFieldNotInitialized")
    private @NotNull Step next;

    @Override
    public void throwException(@NotNull Throwable t) {
      if (state.compareAndSet(State.EXECUTE, State.FAILED)) {
        throwable = t;
        callExceptionHandler();
      } else {
        logger.warn("{}:{}:{} - Unhandled exception", processor.getClass().getSimpleName(), logStream(context), logTime(context));
      }
    }

    @Override
    public void success() {
      if (state.get() == State.EXECUTE) {
        next.execute();
      }
    }

    private void execute() {
      try {
        processor.process(task, this);
      } catch (Throwable t) {
        throwException(t);
      }
    }
  }

  /**
   * The state of the pipeline.
   */
  public enum State {
    /**
     * The initial state after creating a new task pipeline.
     */
    NEW,
    /**
     * The state short before the execution starts.
     */
    PRE_EXECUTE,
    /**
     * The state while the pipeline executes.
     */
    EXECUTE,
    /**
     * The state while invoking the {@link #onSuccess(ISuccessHandler) success} handler, a cancellation in this state is impossible.
     */
    EXECUTE_SUCCESS_HANDLER,
    /**
     * When the execution failed due to an error.
     */
    FAILED,
    /**
     * When the execution succeeded.
     */
    SUCCEEDED
  }

  private final @NotNull RoutingContext context;
  private final @NotNull TASK task;
  private final AtomicReference<@NotNull State> state = new AtomicReference<>(State.NEW);
  private Step first;
  private Step last;
  private Throwable throwable;
  private ISuccessHandler<EVENT, TASK> successHandler;
  private IErrorHandler<EVENT, TASK> errorHandler;

  /**
   * Creates a new initialized pipelines.
   *
   * @param context the routing context.
   * @param task    the task to process.
   */
  TaskPipeline(final @NotNull RoutingContext context, final @NotNull TASK task) {
    this.context = context;
    this.task = task;
  }

  /**
   * Invokes the method when the pipeline did not produce any error (exception).
   *
   * @param processor the processor to be invoked.
   * @return this.
   * @throws NullPointerException  if the given method is null.
   * @throws IllegalStateException if this chain stage has already been initialized.
   */
  public @NotNull TaskPipeline<EVENT, TASK> then(@NotNull ITaskStep<EVENT, TASK> processor)
      throws NullPointerException, IllegalStateException {
    if (state.get() != State.NEW) {
      throw new IllegalStateException("Modification of the pipeline only allowed in NEW state and only by a single thread");
    }
    final Step step = new Step(processor);
    if (first == null) {
      first = last = step;
    } else {
      last.next = step;
      last = step;
    }
    return this;
  }

  /**
   * Add a success handler.
   *
   * @param successHandler the handler to call when the pipeline finished successfully.
   * @return this.
   */
  public @NotNull TaskPipeline<EVENT, TASK> onSuccess(@NotNull ISuccessHandler<EVENT, TASK> successHandler) {
    if (state.get() != State.NEW) {
      throw new IllegalStateException("Modification of the pipeline only allowed in NEW state and only by a single thread");
    }
    this.successHandler = successHandler;
    return this;
  }

  /**
   * Add an error handler.
   *
   * @param errorHandler the handler to call when the pipeline failed.
   * @return this.
   */
  public @NotNull TaskPipeline<EVENT, TASK> onError(@NotNull IErrorHandler<EVENT, TASK> errorHandler) {
    if (state.get() != State.NEW) {
      throw new IllegalStateException("Modification of the pipeline only allowed in NEW state and only by a single thread");
    }
    this.errorHandler = errorHandler;
    return this;
  }

  private void callExceptionHandler() {
    assert state.get() == State.FAILED;
    try {
      errorHandler.onError(task, throwable);
    } catch (Throwable t) {
      logger.error("{}:{}:{} - Uncaught exception in task-pipeline", logId, logStream(context), logTime(context));
    }
  }

  private void callSuccessHandler() {
    if (state.compareAndSet(State.EXECUTE, State.EXECUTE_SUCCESS_HANDLER)) {
      try {
        successHandler.onSuccess(task);
        state.set(State.SUCCEEDED);
      } catch (Throwable t) {
        assert state.get() == State.EXECUTE_SUCCESS_HANDLER;
        state.set(State.FAILED);
        throwable = t;
        callExceptionHandler();
      }
    }
  }

  /**
   * Returns the current state of the pipeline.
   *
   * @return The current state of the pipeline.
   */
  public @NotNull State getState() {
    return state.get();
  }

  /**
   * Execute the pipeline.
   *
   * @return this.
   * @throws IllegalStateException if the pipeline executed already.
   */
  public @NotNull TaskPipeline<EVENT, TASK> execute() {
    if (!state.compareAndSet(State.NEW, State.PRE_EXECUTE)) {
      throw new IllegalStateException("Execution requires the state NEW, but was " + state.get());
    }
    if (successHandler == null) {
      state.set(State.FAILED);
      throw new IllegalStateException("Missing success handler");
    }
    if (errorHandler == null) {
      state.set(State.FAILED);
      throw new IllegalStateException("Missing error handler");
    }
    state.set(State.EXECUTE);
    if (first != null) {
      last.next = new Step(this::last);
      first.execute();
    } else {
      callSuccessHandler();
    }
    return this;
  }

  private void last(@NotNull TASK task, ICallback callback) {
    assert this.task == task;
    callSuccessHandler();
  }

  /**
   * Cancel the pipeline, invokes the cancellation handler synchronously, if successful.
   *
   * @return true if the cancel was successful; false otherwise.
   */
  public boolean cancel() {
    while (true) {
      final State current = state.get();
      if (current == State.PRE_EXECUTE) {
        Thread.yield();
        continue;
      }
      if (current != State.EXECUTE) {
        return false;
      }
      if (state.compareAndSet(State.EXECUTE, State.FAILED)) {
        throwable = new CancellationException();
        callExceptionHandler();
        return true;
      }
      return false;
    }
  }

}
