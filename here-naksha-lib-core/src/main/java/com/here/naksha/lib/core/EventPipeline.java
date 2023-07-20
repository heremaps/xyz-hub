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

import static com.here.naksha.lib.core.NakshaLogger.currentLogger;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.features.Connector;
import com.here.naksha.lib.core.models.features.Space;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import com.here.naksha.lib.core.models.payload.responses.ErrorResponse;
import com.here.naksha.lib.core.models.payload.responses.XyzError;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Default implementation of an event pipeline that provides an event context. This can be used to send events through a pipeline to be
 * handled by {@link IEventHandler event handlers}.
 */
public class EventPipeline implements IEventContext {

  /**
   * Creates a new uninitialized event pipeline.
   */
  public EventPipeline() {
    pipeline = EMPTY;
  }

  /**
   * Creates a new initialized event pipeline.
   *
   * @param handlers all events handlers to be added upfront.
   */
  public EventPipeline(IEventHandler... handlers) {
    if (handlers != null && handlers.length > 0) {
      pipeline = Arrays.copyOf(handlers, handlers.length + 8);
      end = handlers.length;
    } else {
      pipeline = EMPTY;
    }
  }

  private @Nullable Consumer<XyzResponse> callback;

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
    return this.event != null;
  }

  /**
   * Sets the callback, should not be called while the pipeline is executing, otherwise the behavior is undefined.
   *
   * @param callback The callback to invoke, when the response is available.
   * @return this.
   */
  public @NotNull EventPipeline setCallback(@Nullable Consumer<XyzResponse> callback) {
    this.callback = callback;
    return this;
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
   * @param handler the handler to add.
   * @return this.
   */
  public @NotNull EventPipeline addEventHandler(@NotNull IEventHandler handler) {
    if (end >= pipeline.length) {
      pipeline = Arrays.copyOf(pipeline, pipeline.length + 16);
    }
    pipeline[end++] = handler;
    return this;
  }

  /**
   * Add all connectors declared for the given space.
   *
   * @param space The space for which to add the handler.
   * @return this.
   * @throws XyzErrorException If any error occurred.
   */
  public @NotNull EventPipeline addSpaceConnectors(@NotNull Space space) {
    final @Nullable List<@NotNull String> connectorIds = space.getConnectorIds();
    final int SIZE;
    //noinspection ConstantConditions
    if (connectorIds == null || (SIZE = connectorIds.size()) == 0) {
      throw new XyzErrorException(
          XyzError.ILLEGAL_ARGUMENT,
          "The configuration of space " + space.getId() + " is missing the 'connectors'");
    }
    final @NotNull IEventHandler @NotNull [] handlers = new IEventHandler[SIZE];
    for (int i = 0; i < SIZE; i++) {
      final String eventHandlerId = connectorIds.get(i);
      //noinspection ConstantConditions
      if (eventHandlerId == null) {
        throw new XyzErrorException(XyzError.EXCEPTION, "The connector[" + i + "] is null");
      }
      final Connector eventHandler;
      try {
        eventHandler = INaksha.get().connectorReader().getFeatureById(eventHandlerId);
      } catch (Exception e) {
        throw new XyzErrorException(
            XyzError.EXCEPTION,
            "The connector[" + i + "] with id " + eventHandlerId + " failed to read handler",
            e);
      }
      if (eventHandler == null) {
        throw new XyzErrorException(
            XyzError.EXCEPTION, "The connector[" + i + "] with id " + eventHandlerId + " does not exists");
      }
      try {
        handlers[i] = eventHandler.newInstance();
      } catch (Exception e) {
        throw new XyzErrorException(
            XyzError.EXCEPTION,
            "Failed to create an instance of the connector[" + i + "]: " + connectorIds,
            e);
      }
    }

    // Add the handlers and done.
    for (final IEventHandler handler : handlers) {
      addEventHandler(handler);
    }
    return this;
  }

  /**
   * Add the event handlers for the given connector.
   *
   * @param connector the connector for which to add the event handler.
   * @return this.
   */
  public @NotNull EventPipeline addConnector(@NotNull Connector connector) {
    addEventHandler(connector.newInstance());
    return this;
  }

  /**
   * Send a new event through the pipeline.
   *
   * @param event the event to send.
   * @return the generated response.
   * @throws IllegalStateException if the pipeline is already in use.
   */
  public @NotNull XyzResponse sendEvent(@NotNull Event event) {
    if (this.event != null) {
      throw new IllegalStateException("Event already sent");
    }
    this.event = event;
    addEventHandler(this::pipelineEnd);
    this.next = 0;
    XyzResponse response;
    response = sendUpstream(event);
    try {
      if (callback != null) {
        callback.accept(response);
      }
    } catch (Throwable t) {
      currentLogger()
          .atWarn("Uncaught exception in event pipeline callback")
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
  }

  @Override
  public final @NotNull Event getEvent() {
    return event;
  }

  @Override
  public final @NotNull Event setEvent(@NotNull Event newEvent) {
    final Event oldEvent = this.event;
    this.event = newEvent;
    return oldEvent;
  }

  @Override
  public @NotNull XyzResponse sendUpstream() {
    if (next >= pipeline.length) {
      return notImplemented();
    }
    final IEventHandler handler = this.pipeline[next];
    next++;
    if (handler == null) {
      currentLogger()
          .atWarn("Pipeline handler[{}] is null, skip it")
          .add(next - 1)
          .setCause(new NullPointerException())
          .log();
      return sendUpstream(event);
    }
    try {
      return handler.processEvent(this);
    } catch (Throwable t) {
      currentLogger()
          .atWarn("Event processing failed at handler #{}")
          .add(next - 1)
          .setCause(t)
          .log();
      return new ErrorResponse(t, event.getStreamId());
    }
  }

  /**
   * Added to the end of the pipeline, returns that the event is unimplemented.
   *
   * @param eventContext this.
   * @return an unimplemented error response.
   */
  private @NotNull XyzResponse pipelineEnd(@NotNull IEventContext eventContext) {
    currentLogger()
        .atInfo("End of pipeline reached and no handle created a response")
        .setCause(new NullPointerException())
        .log();
    return notImplemented();
  }

  private @NotNull XyzResponse notImplemented() {
    return new ErrorResponse()
        .withStreamId(event.getStreamId())
        .withError(XyzError.NOT_IMPLEMENTED)
        .withErrorMessage("Event '" + event.getClass().getSimpleName() + "' is not supported");
  }
}
