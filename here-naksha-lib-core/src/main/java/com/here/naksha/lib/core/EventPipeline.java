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
package com.here.naksha.lib.core;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.EventFeature;
import com.here.naksha.lib.core.models.XyzError;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.storage.IReadTransaction;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of an event pipeline that provides an event context.
 *
 * <p>Note: The event-pipeline must not be used concurrently by multiple threads, but it is fine if one thread initializes the pipeline and
 * another sends the event. The thread that invokes {@link #sendEvent(Event)} will acquire a lock to the pipeline until the event has
 * finished processing. While processing an event, any other thread that tries to perform any modification to the pipeline will be facing a
 * {@link IllegalStateException}. When multiple threads concurrently try to modify the pipeline this may lead to the same exception!
 */
@SuppressWarnings({"unused", "UnusedReturnValue"})
public class EventPipeline extends NakshaBound {

  class EventContext implements IEventContext {

    EventContext(@NotNull Event event) {
      this.event = event;
    }

    @NotNull
    Event event;

    @Override
    public @NotNull Event getEvent() {
      // This must only be called from within the pipeline, prevent calling from outside!
      if (!mutex.isHeldByCurrentThread()) {
        throw new IllegalStateException(LOCKED_MSG);
      }
      return event;
    }

    @Override
    public @NotNull Event setEvent(@NotNull Event event) {
      // This must only be called from within the pipeline, prevent calling from outside!
      if (!mutex.isHeldByCurrentThread()) {
        throw new IllegalStateException(LOCKED_MSG);
      }
      final Event oldEvent = this.event;
      this.event = event;
      return oldEvent;
    }

    @Override
    public @NotNull XyzResponse sendUpstream() {
      // This must only be called from within the pipeline, prevent calling from outside!
      if (!mutex.isHeldByCurrentThread()) {
        throw new IllegalStateException(LOCKED_MSG);
      }
      if (next >= pipeline.length) {
        return notImplemented(this);
      }
      final IEventHandler handler = pipeline[next];
      next++;
      if (handler == null) {
        log.atWarn()
            .setMessage("Pipeline handler[{}] is null, skip it")
            .addArgument(next - 1)
            .setCause(new NullPointerException())
            .log();
        return sendUpstream(event);
      }
      try {
        return handler.processEvent(this);
      } catch (Throwable t) {
        log.atWarn()
            .setMessage("Event processing failed at handler #{}")
            .addArgument(next - 1)
            .setCause(t)
            .log();
        return new ErrorResponse(t, event.getStreamId());
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
   * The reentrant lock for this pipeline, hold by the thread that calls {@link #sendEvent(Event)}.
   */
  private final ReentrantLock mutex = new ReentrantLock();

  private @Nullable Consumer<XyzResponse> callback;

  /**
   * The creation time of the pipeline.
   */
  private static final IEventHandler[] EMPTY = new IEventHandler[0];

  /**
   * The event that is currently processed.
   */
  private EventContext eventContext;

  private @NotNull IEventHandler @NotNull [] pipeline;
  private int next;
  private int end;

  /**
   * Tests whether this event pipeline is currently processing an event.
   *
   * @return true if this pipeline processing an event; false otherwise.
   */
  public boolean isRunning() {
    return eventContext != null;
  }

  /**
   * Sets the callback, should not be called while the pipeline is executing, otherwise the behavior is undefined.
   *
   * @param callback The callback to invoke, when the response is available.
   * @return this.
   */
  public @NotNull EventPipeline setCallback(@Nullable Consumer<XyzResponse> callback) {
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
  public @Nullable Consumer<XyzResponse> getCallback() {
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
   * @param connector The connector for which to add the event handler.
   * @return This.
   */
  public @NotNull EventPipeline addEventHandler(@NotNull Connector connector) {
    lock();
    try {
      addEventHandler(connector.newInstance());
      return this;
    } finally {
      mutex.unlock();
    }
  }

  /**
   * Add all connectors declared for the given event-feature.
   *
   * @param eventFeature The feature that is logically an event sink.
   * @return This.
   * @throws XyzErrorException If any error occurred.
   */
  public @NotNull EventPipeline addEventHandler(@NotNull EventFeature eventFeature) {
    lock();
    try {
      final @Nullable List<@NotNull String> connectorIds = eventFeature.getConnectorIds();
      final int SIZE;
      //noinspection ConstantConditions
      if (connectorIds == null || (SIZE = connectorIds.size()) == 0) {
        throw new XyzErrorException(
            XyzError.ILLEGAL_ARGUMENT,
            "The configuration of space " + eventFeature.getId() + " is missing the 'connectors'");
      }
      final @NotNull IEventHandler @NotNull [] handlers = new IEventHandler[SIZE];
      try (final IReadTransaction tx = naksha().storage().openReplicationTransaction(naksha().settings())) {
        for (int i = 0; i < SIZE; i++) {
          final String connectorId = connectorIds.get(i);
          //noinspection ConstantConditions
          if (connectorId == null) {
            throw new XyzErrorException(XyzError.EXCEPTION, "The connector[" + i + "] is null");
          }
          final Connector connector;
          try {
            connector = tx.readFeatures(Connector.class, NakshaAdminCollection.CONNECTORS)
                .getFeatureById(connectorId);
          } catch (Exception e) {
            throw new XyzErrorException(
                XyzError.EXCEPTION,
                "The connector[" + i + "] with id " + connectorId + " failed to read handler",
                e);
          }
          if (connector == null) {
            throw new XyzErrorException(
                XyzError.EXCEPTION,
                "The connector[" + i + "] with id " + connectorId + " does not exists");
          }
          try {
            handlers[i] = connector.newInstance();
          } catch (Exception e) {
            throw new XyzErrorException(
                XyzError.EXCEPTION,
                "Failed to create an instance of the connector[" + i + "]: " + connectorIds,
                e);
          }
        }
      }

      // Add the handlers and done.
      for (final IEventHandler handler : handlers) {
        addEventHandler(handler);
      }
      return this;
    } finally {
      mutex.unlock();
    }
  }

  /**
   * Send a new event through the pipeline.
   *
   * @param event The event to send.
   * @return The generated response.
   * @throws IllegalStateException If the pipeline is already in use.
   */
  public @NotNull XyzResponse sendEvent(@NotNull Event event) {
    lock();
    try {
      XyzResponse response;
      this.eventContext = new EventContext(event);
      addEventHandler(this::pipelineEnd);
      this.next = 0;
      response = eventContext.sendUpstream(event);
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
        this.eventContext = null;
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
   * @param eventContext this.
   * @return an unimplemented error response.
   */
  @NotNull
  XyzResponse pipelineEnd(@NotNull IEventContext eventContext) {
    log.atInfo()
        .setMessage("End of pipeline reached and no handle created a response")
        .setCause(new NullPointerException())
        .log();
    return notImplemented(eventContext);
  }

  @NotNull
  XyzResponse notImplemented(@NotNull IEventContext eventContext) {
    return new ErrorResponse()
        .withError(XyzError.NOT_IMPLEMENTED)
        .withErrorMessage("Event '" + eventContext.getClass().getSimpleName() + "' is not supported")
        .withStreamId(eventContext.getEvent().getStreamId());
  }
}
