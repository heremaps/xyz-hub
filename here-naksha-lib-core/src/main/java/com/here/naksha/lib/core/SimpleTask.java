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
package com.here.naksha.lib.core;

import static com.here.naksha.lib.core.NakshaContext.currentContext;
import static com.here.naksha.lib.core.exceptions.UncheckedException.unchecked;

import com.here.naksha.lib.core.lambdas.Fe;
import com.here.naksha.lib.core.lambdas.Fe0;
import com.here.naksha.lib.core.lambdas.Fe1;
import com.here.naksha.lib.core.lambdas.Fe2;
import com.here.naksha.lib.core.lambdas.Fe3;
import com.here.naksha.lib.core.lambdas.Fe4;
import com.here.naksha.lib.core.lambdas.Fe5;
import java.util.concurrent.Future;
import org.apache.commons.lang3.RandomStringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A simple task implementation.
 *
 * @param <RESULT> The result-type to produce.
 */
public class SimpleTask<RESULT> extends AbstractTask<RESULT, SimpleTask<RESULT>> {

  /**
   * Create a new simply task that does nothing and will, when being started, just throw an {@link IllegalStateException}.
   */
  public SimpleTask() {
    this(null, null);
  }

  /**
   * Create a new simply task that does nothing and will, when being started, just throw an {@link IllegalStateException}.
   * @param streamId The stream-id to use.
   */
  public SimpleTask(@NotNull String streamId) {
    this(
        null,
        new NakshaContext(streamId)
            .withAppId(currentContext().getAppId())
            .withAuthor(currentContext().getAuthor()));
  }

  /**
   * Create a new simply task that does nothing and will, when being started, just throw an {@link IllegalStateException}.
   * @param context The context.
   */
  public SimpleTask(@NotNull NakshaContext context) {
    this(null, context);
  }

  /**
   * Create a new simply task that does nothing and will, when being started, just throw an {@link IllegalStateException}.
   *
   * @param naksha  The reference to the Naksha-Hub, if any is available.
   * @param context The context, if any special should be used, otherwise the one of the current thread is used.
   */
  @SuppressWarnings("DataFlowIssue")
  public SimpleTask(@Nullable INaksha naksha, @Nullable NakshaContext context) {
    super(naksha, context != null ? context : currentContext());
    this.id = RandomStringUtils.randomAlphanumeric(20);
  }

  /**
   * Returns a unique identifier for this task.
   *
   * @return a unique identifier for this task.
   */
  public @NotNull String id() {
    return id;
  }

  protected @NotNull String id;
  protected @Nullable Fe lambda;
  protected @Nullable Object[] args;

  /**
   * Runs the tasks with the given code that should be executed.
   *
   * @param lambda The lambda that produces the result.
   * @return the future of the result.
   */
  public @NotNull Future<RESULT> start(@NotNull Fe0<RESULT> lambda) {
    this.lambda = lambda;
    args = null;
    return start();
  }

  /**
   * Runs the tasks with the given code that should be executed.
   *
   * @param lambda The lambda that produces the result.
   * @param a      First parameter.
   * @return the future of the result.
   */
  public <A> @NotNull Future<RESULT> start(@NotNull Fe1<RESULT, A> lambda, A a) {
    this.lambda = lambda;
    args = new Object[] {a};
    return start();
  }

  /**
   * Runs the tasks with the given code that should be executed.
   *
   * @param lambda The lambda that produces the result.
   * @param a      First parameter.
   * @param b      Second parameter.
   * @return the future of the result.
   */
  public <A, B> @NotNull Future<RESULT> start(@NotNull Fe2<RESULT, A, B> lambda, A a, B b) {
    this.lambda = lambda;
    args = new Object[] {a, b};
    return start();
  }

  /**
   * Runs the tasks with the given code that should be executed.
   *
   * @param lambda The lambda that produces the result.
   * @param a      First parameter.
   * @param b      Second parameter.
   * @param c      Third parameter.
   * @return the future of the result.
   */
  public <A, B, C> @NotNull Future<RESULT> start(@NotNull Fe3<RESULT, A, B, C> lambda, A a, B b, C c) {
    this.lambda = lambda;
    args = new Object[] {a, b, c};
    return start();
  }

  /**
   * Runs the tasks with the given code that should be executed.
   *
   * @param lambda The lambda that produces the result.
   * @param a      First parameter.
   * @param b      Second parameter.
   * @param c      Third parameter.
   * @param d      Fourth parameter.
   * @return the future of the result.
   */
  public <A, B, C, D> @NotNull Future<RESULT> start(@NotNull Fe4<RESULT, A, B, C, D> lambda, A a, B b, C c, D d) {
    this.lambda = lambda;
    args = new Object[] {a, b, c, d};
    return start();
  }

  /**
   * Runs the tasks with the given code that should be executed.
   *
   * @param lambda The lambda that produces the result.
   * @param a      First parameter.
   * @param b      Second parameter.
   * @param c      Third parameter.
   * @param d      Fourth parameter.
   * @param e      Fifth parameter.
   * @return the future of the result.
   */
  public <A, B, C, D, E> @NotNull Future<RESULT> start(
      @NotNull Fe5<RESULT, A, B, C, D, E> lambda, A a, B b, C c, D d, E e) {
    this.lambda = lambda;
    args = new Object[] {a, b, c, d, e};
    return start();
  }

  @Override
  protected void init() {}

  @SuppressWarnings({"rawtypes", "unchecked"})
  @Override
  protected @NotNull RESULT execute() {
    if (lambda != null) {
      try {
        if (lambda instanceof Fe0) {
          return ((Fe0<RESULT>) lambda).call();
        }
        if (lambda instanceof Fe1) {
          assert args != null && args.length == 1;
          return ((Fe1<RESULT, Object>) lambda).call(args[0]);
        }
        if (lambda instanceof Fe2) {
          assert args != null && args.length == 2;
          return ((Fe2<RESULT, Object, Object>) lambda).call(args[0], args[1]);
        }
        if (lambda instanceof Fe3) {
          assert args != null && args.length == 3;
          return ((Fe3<RESULT, Object, Object, Object>) lambda).call(args[0], args[1], args[2]);
        }
        if (lambda instanceof Fe4) {
          assert args != null && args.length == 4;
          return ((Fe4<RESULT, Object, Object, Object, Object>) lambda)
              .call(args[0], args[1], args[2], args[3]);
        }
        if (lambda instanceof Fe5) {
          assert args != null && args.length == 5;
          return ((Fe5<RESULT, Object, Object, Object, Object, Object>) lambda)
              .call(args[0], args[1], args[2], args[3], args[4]);
        }
      } catch (Exception e) {
        throw unchecked(e);
      }
    }
    throw new IllegalStateException();
  }
}
