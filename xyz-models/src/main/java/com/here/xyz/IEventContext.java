package com.here.xyz;

import com.here.xyz.events.Event;
import com.here.xyz.responses.XyzResponse;
import org.jetbrains.annotations.NotNull;

/**
 * The event-context that wraps the event and allows arbitrary attachments.
 */
public interface IEventContext {

  /**
   * Returns the event of the context.
   *
   * @return the event of the context.
   */
  @NotNull Event event();

  /**
   * Send the event upstream to the next event handler. If no further handler is available, the default implementation at the end of each
   * pipeline will return a not implemented error response.
   *
   * @param event the event to send upstream.
   * @return the generated response.
   */
  @NotNull XyzResponse sendUpstream(Event event);
}