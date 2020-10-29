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

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A pipeline with functions to process a a task.
 *
 * @param <V> the type of the task.
 */
public class TaskPipeline<V> {

  private final TaskPipeline<V> first;
  private final State<V> state;
  private TaskPipeline<V> next;
  private C2<V, Callback<V>> ifNotNull;
  private C1<V> finish;
  private C2<V, Exception> finishException;
  private AtomicBoolean consumed = new AtomicBoolean(false);

  /**
   * Creates a new pipeline step.
   *
   * @param first the first chain stage.
   * @throws NullPointerException if the given chain stage is null or does not have a callback handler.
   */
  private TaskPipeline(TaskPipeline<V> first) throws NullPointerException {
    this.first = first;
    if (first.state == null) {
      throw new NullPointerException();
    }
    this.state = first.state;
  }

  /**
   * Creates a pipelines.
   *
   * @param task the initial chain value.
   */
  private TaskPipeline(V task) {
    first = this;
    state = new State<>();
    state.next = this;
    state.value = task;
  }

  public static <V> TaskPipeline<V> create(V task) {
    return new TaskPipeline<>(task);
  }

  /**
   * Invokes the method when the chain did not produce any exception and the chain value not null; otherwise the next stage is executed.
   *
   * @param nextFunction the method to be invoked.
   * @return the next stage.
   * @throws NullPointerException if the given method is null.
   * @throws IllegalStateException if this chain stage has already been initialized.
   */
  public TaskPipeline<V> then(C2<V, Callback<V>> nextFunction) throws NullPointerException, IllegalStateException {
    if (next != null) {
      throw new IllegalStateException("The chain stage is already initialized, the same stage can't be handled twice");
    }
    this.ifNotNull = nextFunction;
    next = new TaskPipeline<>(first);
    return next;
  }

  /**
   * Registers a finishing state that will have the on-success method being invoked when the chain did not produce any exception. If the
   * chain produced an exception or the success handler produced an exception, then the provided exception handler is invoked.
   *
   * @param onSuccess the method to be invoked when the chain successfully produced a result.
   * @param onException the method to be invoked when the chain produced an exception or the on-success handler produced an exception.
   * @return the first chain stage.
   * @throws NullPointerException if the given method is null.
   * @throws IllegalStateException if this chain stage has already been initialized.
   */
  public TaskPipeline<V> finish(C1<V> onSuccess, C2<V, Exception> onException) throws NullPointerException, IllegalStateException {
    if (onSuccess == null) {
      throw new NullPointerException("nextFunction");
    }
    if (next != null) {
      throw new IllegalStateException("The chain stage is already initialized, the same stage can't be handled twice");
    }
    this.finish = onSuccess;
    this.finishException = onException;
    return first;
  }

  /**
   * Execute the chain and return the first chain stage.
   *
   * @return the first chain stage.
   * @throws IllegalStateException if the chain was already executed or if the chain was created uninitialized, so without an initial
   * value.
   */
  public TaskPipeline<V> execute() throws IllegalStateException {
    if (state.isExecuted) {
      throw new IllegalStateException("The chain was already executed");
    }
    if (first.state.value == null) {
      throw new IllegalStateException("The chain was not initialized with a value, please invoke execute(chainValue)");
    }
    state.isExecuted = true;
    first._execute();
    return first;
  }

  /**
   * Execute the chain with the given initial value and return the first chain stage.
   *
   * @param chainValue the initial value with which to execute the chain.
   * @return the first chain stage.
   * @throws IllegalStateException if the chain was already executed or the chain was created with an initial value.
   */
  public TaskPipeline<V> execute(V chainValue) throws IllegalStateException {
    if (state.isExecuted) {
      throw new IllegalStateException("The chain was already executed");
    }
    if (first.state.value != null) {
      throw new IllegalStateException("The chain is already initialized with a value");
    }
    state.isExecuted = true;
    first.state.value = chainValue;
    first._execute();
    return first;
  }

  private void _execute() throws IllegalStateException {
    if (!consumed.compareAndSet(false, true)) {
      throw new IllegalStateException("This chain stage was already consumed");
    }

    state.next = next;

    // If there is no exception present.
    if (state.exception == null) {
      try {
        if (this.ifNotNull != null && state.value != null) {
          this.ifNotNull.call(state.value, state);
          return;
        }

        if (this.finish != null) {
          this.finish.call(state.value);
          return;
        }
      } catch (Exception e) {
        state.exception = e;
      }
    }

    // If there is an exception present.
    if (state.exception != null) {
      if (this.finishException != null) {
        try {
          final Exception theException = state.exception;
          state.exception = null;
          this.finishException.call(state.value, theException);
          return;
        } catch (Exception e) {
          state.exception = e;
        }
      }
    }

    //Execute the next chain stage, if there is any.
    if (next != null && !state.isCancelled) {
      next._execute();
    }
  }

  void cancel() {
    state.isCancelled = true;
  }

  /**
   * The callback handler.
   *
   * @param <V> the type of the value of the chain.
   */
  public interface Callback<V> {

    /**
     * Report an exception, the current chain value stays what it is.
     *
     * @param e the exception to report.
     */
    void exception(Exception e);

    /**
     * Report a new chain value and clears the current exception.
     *
     * @param value the new value to report.
     */
    void call(V value);
  }

  @FunctionalInterface
  public interface C1<A> {

    void call(A a) throws Exception;
  }

  @FunctionalInterface
  public interface C2<A, B> {

    void call(A a, B b) throws Exception;
  }

  /**
   * The state of the chain that implements as well the callback handler.
   *
   * @param <V> the type of the value of the chain.
   */
  private static class State<V> implements Callback<V> {

    private boolean isCancelled;
    private boolean isExecuted;
    private V value;
    private Exception exception;
    private TaskPipeline<V> next;

    @Override
    public void exception(Exception e) {
      this.exception = e;
      if (next != null && !isCancelled) {
        next._execute();
      }
    }

    @Override
    public void call(V value) {
      this.value = value;
      if (next != null && !isCancelled) {
        next._execute();
      }
    }
  }
}
