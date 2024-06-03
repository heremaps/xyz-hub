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

import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.naksha.EventHandler;
import com.here.naksha.lib.core.models.storage.ErrorResult;
import com.here.naksha.lib.core.models.storage.Request;
import com.here.naksha.lib.core.models.storage.Result;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of an event pipeline that wraps {@link Request requests} into an event.
 *
 * <p>Note: The event-pipeline must not be used concurrently by multiple threads, but it is fine if one thread initializes the pipeline and
 * another sends the event. The thread that invokes {@link #sendEvent(Request)} will acquire a lock to the pipeline until the event has
 * finished processing. While processing an event, any other thread that tries to perform any modification to the pipeline will be facing a
 * {@link IllegalStateException}. When multiple threads concurrently try to modify the pipeline this may lead to the same exception!
 */
@SuppressWarnings({"unused", "UnusedReturnValue"})
public class EventPipeline extends NakshaBound {

  class Event implements IEvent {

    Event(@NotNull Request<?> request) {
      this.request = request;
    }

    @NotNull
    Request<?> request;

    @Override
    public @NotNull Request<?> getRequest() {
      // This must only be called from within the pipeline, prevent calling from outside!
      if (!mutex.isHeldByCurrentThread()) {
        throw new IllegalStateException(LOCKED_MSG);
      }
      return request;
    }

    @Override
    public @NotNull Request<?> setRequest(@NotNull Request<?> request) {
      // This must only be called from within the pipeline, prevent calling from outside!
      if (!mutex.isHeldByCurrentThread()) {
        throw new IllegalStateException(LOCKED_MSG);
      }
      final Request<?> oldRequest = this.request;
      this.request = request;
      return oldRequest;
    }

    @Override
    public @NotNull Result sendUpstream() {
      // This must only be called from within the pipeline, prevent calling from outside!
      if (!mutex.isHeldByCurrentThread()) {
        throw new IllegalStateException(LOCKED_MSG);
      }
      if (next >= pipeline.length) {
        return notImplemented(this);
      }
      final IEventHandler handler = pipeline[next];
      next++;
      //noinspection ConstantValue
      if (handler == null) {
        log.atWarn()
            .setMessage("Pipeline handler[{}] is null, skip it")
            .addArgument(next - 1)
            .setCause(new NullPointerException())
            .log();
        return sendUpstream(request);
      }
      try {
        return handler.processEvent(this);
      } catch (Throwable t) {
        final String msg = "Event processing failed at handler #" + (next - 1) + " ["
            + handler.getClass().getSimpleName() + "]. " + t.getMessage();
        log.atWarn().setMessage(msg).setCause(t).log();
        return new ErrorResult(XyzError.EXCEPTION, msg, t);
      }
    }
  }

  private static final Logger log = LoggerFactory.getLogger(EventPipeline.class);
  private static final String LOCKED_MSG = "The pipeline is locked";
  private static final long TRY_TIME = 10;
  private static final TimeUnit TRY_TIME_UNIT = TimeUnit.MILLISECONDS;

  /**
   * Creates a new uninitialized event pipeline.
   *
   * @param naksha The reference to the Naksha host.
   */
  public EventPipeline(@NotNull INaksha naksha) {
    super(naksha);
    pipeline = EMPTY;
  }

  /**
   * Creates a new initialized event pipeline.
   *
   * @param naksha   The reference to the Naksha host.
   * @param handlers all events handlers to be added upfront.
   */
  public EventPipeline(@NotNull INaksha naksha, @NotNull IEventHandler... handlers) {
    super(naksha);
    if (handlers != null && handlers.length > 0) {
      pipeline = Arrays.copyOf(handlers, handlers.length + 8);
      end = handlers.length;
    } else {
      pipeline = EMPTY;
    }
  }

  /**
   * The reentrant lock for this pipeline, hold by the thread that calls {@link #sendEvent(Request)}.
   */
  private final ReentrantLock mutex = new ReentrantLock();

  private @Nullable Consumer<Result> callback;

  /**
   * The creation time of the pipeline.
   */
  private static final IEventHandler[] EMPTY = new IEventHandler[0];

  /**
   * The event that is currently processed.
   */
  private Event event;

  private @NotNull IEventHandler @NotNull [] pipeline;
  private int next;
  private int end;

  /**
   * Tests whether this event pipeline is currently processing an event.
   *
   * @return true if this pipeline processing an event; false otherwise.
   */
  public boolean isRunning() {
    return event != null;
  }

  /**
   * Sets the callback, should not be called while the pipeline is executing, otherwise the behavior is undefined.
   *
   * @param callback The callback to invoke, when the response is available.
   * @return this.
   */
  public @NotNull EventPipeline setCallback(@Nullable Consumer<Result> callback) {
    lock();
    try {
      this.callback = callback;
      return this;
    } finally {
      mutex.unlock();
    }
  }

  /**
   * Returns the currently set callback.
   *
   * @return The currently set callback.
   */
  public @Nullable Consumer<Result> getCallback() {
    return callback;
  }

  /**
   * Add the given handler to the pipeline.
   *
   * @param handler The handler to add.
   * @return This.
   */
  public @NotNull EventPipeline addEventHandler(@NotNull IEventHandler handler) {
    lock();
    try {
      if (end >= pipeline.length) {
        pipeline = Arrays.copyOf(pipeline, pipeline.length + 16);
      }
      pipeline[end++] = handler;
      return this;
    } finally {
      mutex.unlock();
    }
  }

  /**
   * Add the event handler for the given connector.
   *
   * @param eventHandler The connector for which to add the event handler.
   * @return This.
   */
  public @NotNull EventPipeline addEventHandler(@NotNull EventHandler eventHandler) {
    lock();
    try {
      addEventHandler(eventHandler.newInstance(naksha()));
      return this;
    } finally {
      mutex.unlock();
    }
  }

  /**
   * Send a new event through the pipeline.
   *
   * @param request The event to send.
   * @return The generated response.
   * @throws IllegalStateException If the pipeline is already in use.
   */
  public @NotNull Result sendEvent(@NotNull Request<?> request) {
    lock();
    try {
      Result response;
      this.event = new Event(request);
      // addEventHandler(this::pipelineEnd);
      addEventHandler(new EndPipelineHandler());
      this.next = 0;
      response = event.sendUpstream(request);
      try {
        if (callback != null) {
          callback.accept(response);
        }
      } catch (Throwable t) {
        log.atWarn()
            .setMessage("Uncaught exception in event pipeline callback")
            .setCause(t)
            .log();
      } finally {
        callback = null;
        this.event = null;
        pipeline = EMPTY;
        next = 0;
        end = 0;
      }
      return response;
    } finally {
      mutex.unlock();
    }
  }

  void lock() {
    try {
      if (!mutex.tryLock(TRY_TIME, TRY_TIME_UNIT)) {
        throw new IllegalStateException(LOCKED_MSG);
      }
    } catch (InterruptedException e) {
      throw new IllegalStateException(LOCKED_MSG);
    }
  }

  /**
   * Added to the end of the pipeline, returns that the event is unimplemented.
   *
   * @param event this.
   * @return an unimplemented error response.
   * @deprecated use EndPipelineHandler instead, which is easier to test (than this lambda based IEventHandler function)
   */
  @Deprecated
  @NotNull
  Result pipelineEnd(@NotNull IEvent event) {
    log.atInfo()
        .setMessage("End of pipeline reached and no handle created a response")
        .setCause(new IllegalStateException())
        .log();
    return notImplemented(event);
  }

  @NotNull
  Result notImplemented(@NotNull IEvent event) {
    return new ErrorResult(
        XyzError.NOT_IMPLEMENTED, "Event '" + event.getClass().getSimpleName() + "' is not supported");
  }
}
