/*
 * Copyright (C) 2017-2021 HERE Europe B.V.
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

import static com.here.xyz.hub.task.TaskState.CANCELLED;
import static com.here.xyz.hub.task.TaskState.ERROR;
import static com.here.xyz.hub.task.TaskState.INIT;
import static com.here.xyz.hub.task.TaskState.IN_PROGRESS;
import static com.here.xyz.hub.task.TaskState.RESPONSE_SENT;
import static com.here.xyz.hub.task.TaskState.STARTED;

import com.here.xyz.EventPipeline;
import com.here.xyz.events.Event;
import com.here.xyz.hub.auth.JWTPayload;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.rest.Context;
import com.here.xyz.responses.XyzResponse;
import io.vertx.ext.web.RoutingContext;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A task represents a pipeline to process an event.
 */
public abstract class Task extends EventPipeline {

  protected static final Logger logger = LoggerFactory.getLogger(Task.class);

  protected Task(@NotNull String name) {
    this.name = name;
  }

  /**
   * The corresponding routing context.
   */
  public final @NotNull RoutingContext context;

  /**
   * The response type that should be produced by this task.
   */
  public @NotNull ApiResponseType responseType;

  /**
   * Describes, if the response was loaded from cache.
   */
  private boolean cacheHit;

  /**
   * The event to process.
   * <p>
   * After the event has been {@link #consumeEvent() consumed}, the event gets deleted from memory and {@link #getEvent()} and
   * {@link #consumeEvent()} will throw a {@link IllegalStateException}.
   */
  private final AtomicReference<EVENT> event = new AtomicReference<>();

  /**
   * A local copy of {@link Event#getIfNoneMatch()}.
   */
  private final @Nullable String ifNoneMatch;

  /**
   * Indicates, if the task was executed.
   */
  private boolean executed;

  /**
   * The current state / phase of this task. The state can be read to know whether an action should still be performed or may be cancelled.
   * E.g. if the task is in a final state already it doesn't make sense to send a(nother) response or fail with another exception.
   *
   * @see TaskState
   */
  private @NotNull TaskState state = INIT;

  private final @NotNull TaskPipeline<EVENT, TASK> pipeline;

  public final @NotNull String logId() {
    return getClass().getSimpleName();
  }

  public final @NotNull String logStream() {
    return Context.logStream(context);
  }

  public long logTime() {
    return Context.logTime(context);
  }

  private final ConcurrentHashMap<Consumer<TASK>, Boolean> cancellingHandlers = new ConcurrentHashMap<>();

  /**
   * @throws NullPointerException if the given context or responseType are null.
   */
  public Task(
      final @NotNull EVENT event,
      final @NotNull RoutingContext context,
      final @NotNull ApiResponseType responseType
  ) throws NullPointerException {
    event.setIfNoneMatch(context.request().headers().get("If-None-Match"));
    event.setStreamId(Context.logStream(context));
    this.event.set(event);
    this.ifNoneMatch = event.getIfNoneMatch();
    this.context = context;
    this.responseType = responseType;
    Context.setTask(context, this);
    this.pipeline = new TaskPipeline<>(context, self());
  }

  /**
   * Returns this task.
   * @return This task.
   */
  @SuppressWarnings("unchecked")
  public final @NotNull TASK self() {
    return (TASK) this;
  }

  public @NotNull EVENT getEvent() throws IllegalStateException {
    final EVENT event = this.event.get();
    if (event == null) {
      throw new IllegalStateException("Event was already consumed.");
    }
    return event;
  }

  /**
   * Finally consumes the event. Calling this method the event being bound to this task will be returned and all internal references are
   * deleted. After the event has been been consumed neither {@link #getEvent()} nor {@link #consumeEvent()} may be called anymore.
   * Otherwise an {@link IllegalStateException} will be thrown.
   *
   * @throws IllegalStateException In case the event was consumed already
   */
  public @NotNull EVENT consumeEvent() throws IllegalStateException {
    final EVENT event = this.event.getAndSet(null);
    if (event == null) {
      throw new IllegalStateException("Event was already consumed");
    }
    state = IN_PROGRESS;
    return event;
  }

  public boolean etagMatches() {
    return XyzResponse.etagMatches(ifNoneMatch, getEtag());
  }

  public void execute(
      final @NotNull ISuccessHandler<EVENT, TASK> onSuccess,
      final @NotNull IErrorHandler<EVENT, TASK> onError
  ) {
    if (!executed) {
      executed = true;
      final TaskPipeline<EVENT, TASK> pipeline = getPipeline();
      pipeline.onSuccess((task) -> {
        if (state.isFinal()) {
          return;
        }
        onSuccess.onSuccess(task);
        state = RESPONSE_SENT;
      });
      pipeline.onError((task, exception) -> {
        if (state.isFinal()) {
          return;
        }
        state = ERROR;
        onError.onError(task, exception);
      });
      pipeline.execute();
    }
  }

  public @NotNull String streamId() {
    return Context.logStream(context);
  }

  /**
   * Returns the payload of the JWT Token.
   *
   * @return the payload of the JWT Token
   */
  public @Nullable JWTPayload getJwt() {
    return Context.jwt(context);
  }

  /**
   * Creates the execution pipeline.
   *
   * @param pipeline the pipeline that should be initialized.
   */
  protected abstract void initPipeline(@NotNull TaskPipeline<EVENT, TASK> pipeline);

  /**
   * Returns the (previously) created execution pipeline.
   *
   * @return The (previously) created execution pipeline.
   */
  public final @NotNull TaskPipeline<EVENT, TASK> getPipeline() {
    return pipeline;
  }

  /**
   * Returns the e-tag of the response, if there is any available.
   *
   * @return the e-tag value.
   */
  public @Nullable String getEtag() {
    // TODO: Should we not look this up in the context.response()?
    return null;
  }

  public boolean isCacheHit() {
    return cacheHit;
  }

  public void setCacheHit(boolean cacheHit) {
    this.cacheHit = cacheHit;
  }

  public @NotNull TaskState getState() {
    if (state == INIT && executed) {
      return STARTED;
    }
    return state;
  }

  public void addCancellingHandler(final @NotNull Consumer<@NotNull TASK> cancellingHandler) {
    Objects.requireNonNull(cancellingHandler);
    cancellingHandlers.put(cancellingHandler, true);
  }

  public void cancel() {
    if (!state.isFinal()) {
      try {
        //Cancel all further steps in the pipeline
        getPipeline().cancel();
        /*
        Call all registered CancellingHandlers
        (e.g. to cancel running / pending requests which might have been started by previous actions already)
         */
        final Enumeration<Consumer<TASK>> keyEnum = cancellingHandlers.keys();
        while (keyEnum.hasMoreElements()) {
          try {
            final Consumer<TASK> taskConsumer = keyEnum.nextElement();
            taskConsumer.accept(self());
          } catch (NoSuchElementException | NullPointerException ignore) {

          } catch (Exception e) {
            logger.error("{}:{}:{} - Uncaught exception in cancelling handler", logId(), logStream(), logTime(), e);
          }
        }
      } catch (Exception e) {
        logger.error("{}:{}:{} - Error cancelling the task.", logId(), logStream(), logTime(), e);
      } finally {
        state = CANCELLED;
      }
    }
  }
}
