package com.here.xyz;

import com.here.xyz.events.Event;
import com.here.xyz.responses.XyzResponse;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

/**
 * The event-context that wraps the event.
 */
public interface IEventContext {

  /**
   * Returns the logger (as provided by the host) for logging.
   *
   * @return the logger (as provided by the host) for logging.
   */
  @NotNull Logger logger();

  /**
   * Returns the event of the context.
   *
   * @return the event of the context.
   */
  @NotNull Event<?> event();

  /**
   * Send the event upstream to the next event handler. If no further handler is available, the default implementation at the end of each
   * pipeline will return a not implemented error response.
   *
   * @return the generated response.
   */
  @NotNull XyzResponse<?> sendUpstream();
}