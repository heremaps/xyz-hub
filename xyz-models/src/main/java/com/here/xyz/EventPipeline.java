package com.here.xyz;

import com.here.xyz.events.Event;
import com.here.xyz.responses.ErrorResponse;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Default implementation of an event context based upon a given list of event handler.
 */
public class EventPipeline implements IEventContext {

  protected static final Logger logger = LoggerFactory.getLogger(EventPipeline.class);

  /**
   * Creates a new event context for the given event and the given event-handler.
   *
   * @param event    the event to wrap.
   * @param pipeline the handler that should form the pipeline.
   */
  public EventPipeline(@NotNull Event<?> event, @NotNull IEventHandler @NotNull [] pipeline) {
    this.event = event;
    this.pipeline = pipeline;
  }

  /**
   * Creates a new event context for the given event and the given event-handler.
   *
   * @param event     the event to wrap.
   * @param pipeline  the handler that should form the pipeline.
   * @param processor the processor to finally handle the event (basically the storage).
   */
  public EventPipeline(@NotNull Event<?> event, @NotNull IEventHandler @Nullable [] pipeline, @NotNull IEventHandler processor) {
    this.event = event;
    if (pipeline != null) {
      this.pipeline = Arrays.copyOf(pipeline, pipeline.length + 1);
      this.pipeline[this.pipeline.length - 1] = processor;
    } else {
      this.pipeline = new IEventHandler[]{processor};
    }
  }

  protected final @NotNull Event<?> event;
  protected final @NotNull IEventHandler @NotNull [] pipeline;
  protected int i = 0;

  @Override
  public @NotNull Logger logger() {
    return logger;
  }

  @Override
  public @NotNull Event<?> event() {
    return event;
  }

  @Override
  public @NotNull XyzResponse<?> sendUpstream() {
    if (i >= pipeline.length) {
      return new ErrorResponse()
          .withStreamId(event.getStreamId())
          .withError(XyzError.NOT_IMPLEMENTED)
          .withErrorMessage("Event not implemented: '" + event.getClass().getSimpleName() + "'");
    }
    final IEventHandler handler = this.pipeline[i];
    i++;
    if (handler == null) {
      logger.error("{}:{}:{} - Pipeline handler[{}] is null, skip it",
          event.logId(), event.logTime(), event.logId(), i - 1, new NullPointerException());
      return sendUpstream();
    } else {
      logger.error("{}:{}:{} - End of pipeline reached and no handle created a response",
          event.logId(), event.logTime(), event.logId(), new NullPointerException());
      return handler.processEvent(this);
    }
  }
}