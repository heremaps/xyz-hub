package com.here.xyz;

import com.here.xyz.events.Event;
import java.util.Arrays;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A simple pipeline that can be used to process all events with the same pipeline. Mainly for testing purpose, because normally the
 * connector defines the pipeline to be used.
 */
public class SimpleIoPipeline extends AbstractIoPipeline {

  /**
   * Creates a new event context for the given event and the given event-handler.
   *
   * @param pipeline the handler that should form the pipeline.
   */
  public SimpleIoPipeline(@NotNull IEventHandler @NotNull [] pipeline) {
    this.pipeline = pipeline;
  }

  /**
   * Creates a new event context for the given event and the given event-handler.
   *
   * @param processor the processor to finally handle the event (basically the storage).
   */
  public SimpleIoPipeline(@NotNull IEventHandler processor) {
    this(null, processor);
  }

  /**
   * Creates a new event context for the given event and the given event-handler.
   *
   * @param pipeline  the handler that should form the pipeline.
   * @param processor the processor to finally handle the event (basically the storage).
   */
  public SimpleIoPipeline(@NotNull IEventHandler @Nullable [] pipeline, @NotNull IEventHandler processor) {
    if (pipeline != null) {
      this.pipeline = Arrays.copyOf(pipeline, pipeline.length + 1);
      this.pipeline[this.pipeline.length - 1] = processor;
    } else {
      this.pipeline = new IEventHandler[]{processor};
    }
  }

  protected final @NotNull IEventHandler @NotNull [] pipeline;

  @Override
  protected @NotNull IEventHandler @NotNull [] pipeline(@NotNull Event<?> event) {
    return pipeline;
  }
}
