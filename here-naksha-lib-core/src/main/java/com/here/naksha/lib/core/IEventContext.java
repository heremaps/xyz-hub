package com.here.naksha.lib.core;

import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import org.jetbrains.annotations.NotNull;

/** The event-context that wraps the event and allows arbitrary attachments. */
@SuppressWarnings("UnusedReturnValue")
public interface IEventContext {

  /**
   * Returns the current event of the underlying pipeline.
   *
   * @return the current event of the underlying pipeline.
   */
  @NotNull
  Event getEvent();

  /**
   * Replace the event in the pipeline, returning the old even.
   *
   * @param event The new event.
   * @return The previous event.
   */
  @NotNull
  Event setEvent(@NotNull Event event);

  /**
   * Send the event upstream to the next event handler. If no further handler is available, the
   * default implementation at the end of each pipeline will return a not implemented error
   * response.
   *
   * @return the generated response.
   */
  @NotNull
  XyzResponse sendUpstream();

  /**
   * Change the event and send it upstream to the next event handler. If no further handler is
   * available, the default implementation at the end of each pipeline will return a not implemented
   * error response.
   *
   * <p>Note: Calling this method is the same as:
   *
   * <pre>{@code
   * context.setEvent(newEvent);
   * context.sendUpstream();
   * }</pre>
   *
   * @param event the event to send upstream.
   * @return the generated response.
   */
  default @NotNull XyzResponse sendUpstream(@NotNull Event event) {
    setEvent(event);
    return sendUpstream();
  }
}
