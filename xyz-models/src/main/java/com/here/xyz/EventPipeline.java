package com.here.xyz;

import static com.here.xyz.EventTask.currentTask;

import com.here.xyz.events.Event;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.models.hub.Space;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
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
   * Add all declared event handler, and the storage connector to this pipeline.
   *
   * @param space The space for which to add the handler.
   * @return this.
   * @throws XyzErrorException If any error occurred.
   */
  public @NotNull EventPipeline addSpaceHandler(@NotNull Space space) throws XyzErrorException {
    final @Nullable List<@NotNull String> connectorIds = space.getConnectorIds();
    final int SIZE;
    if (connectorIds == null || (SIZE = connectorIds.size()) == 0) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT,
          "The configuration of space " + space.getId() + " is missing the 'connectors'");
    }
    final @NotNull EventHandler @NotNull [] handlers = new EventHandler[SIZE];
    for (int i = 0; i < SIZE; i++) {
      final String connectorId = connectorIds.get(i);
      //noinspection ConstantConditions
      if (connectorId == null) {
        throw new XyzErrorException(XyzError.EXCEPTION, "The connector[" + i + "] is null");
      }
      final Connector connector = Connector.getConnectorById(connectorId);
      if (connector == null) {
        throw new XyzErrorException(XyzError.EXCEPTION,
            "The connector[" + i + "] with id " + connectorId + " does not exists");
      }
      try {
        handlers[i] = EventHandler.newInstance(connector);
      } catch (XyzErrorException e) {
        throw new XyzErrorException(XyzError.EXCEPTION,
            "Failed to create an instance of the connector[" + i + "]: " + connectorIds, e);
      }
    }

    // Add the handlers and done.
    for (final EventHandler handler : handlers) {
      addEventHandler(handler);
    }
    return this;
  }

  /**
   * Add the event handlers for the given connector.
   *
   * @param connector the connector for which to add the event handler.
   * @return this.
   * @throws XyzErrorException If creating a new instance of the connector failed, leaves the pipeline in the state it had before calling
   *                           this method.
   */
  public @NotNull EventPipeline addConnectorHandler(@NotNull Connector connector) throws XyzErrorException {
    addEventHandler(EventHandler.newInstance(connector));
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
    final EventTask context = currentTask();
    this.next = 0;
    XyzResponse response;
    try {
      response = sendUpstream(event);
    } catch (Throwable t) {
      context.error("Uncaught exception in event pipeline", t);
      response = new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.EXCEPTION)
          .withErrorMessage("Unexpected exception in storage connector: " + t.getMessage());
    }
    assert response != null;
    try {
      if (callback != null) {
        callback.accept(response);
      }
    } catch (Throwable t) {
      context.error("Uncaught exception in event pipeline callback", t);
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
  public final @NotNull Event event() {
    return event;
  }

  @Override
  public @NotNull XyzResponse sendUpstream(@NotNull Event event) {
    if (next >= pipeline.length) {
      return notImplemented();
    }
    this.event = event;
    final IEventHandler handler = this.pipeline[next];
    next++;
    if (handler == null) {
      currentTask().error("Pipeline handler[{}] is null, skip it", next - 1, new NullPointerException());
      return sendUpstream(event);
    }
    try {
      return handler.processEvent(this);
    } catch (XyzErrorException e) {
      currentTask().info("Event processing failed at handler #{}", next - 1, e);
      return e.toErrorResponse(event.getStreamId());
    }
  }

  /**
   * Added to the end of the pipeline, returns that the event is unimplemented.
   *
   * @param eventContext this.
   * @return an unimplemented error response.
   */
  private @NotNull XyzResponse pipelineEnd(@NotNull IEventContext eventContext) {
    currentTask().info("End of pipeline reached and no handle created a response", new NullPointerException());
    return notImplemented();
  }

  private @NotNull XyzResponse notImplemented() {
    return new ErrorResponse()
        .withStreamId(event.getStreamId())
        .withError(XyzError.NOT_IMPLEMENTED)
        .withErrorMessage("Event '" + event.getClass().getSimpleName() + "' is not supported");
  }
}