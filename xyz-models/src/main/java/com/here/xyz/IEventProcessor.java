package com.here.xyz;

import com.here.xyz.events.Event;
import com.here.xyz.responses.XyzResponse;
import org.jetbrains.annotations.NotNull;

/**
 * The default interface that must be implemented by all event processors. A processor is code that uses a specific event context provided
 * by the host and bound to that event context, to process the bound event. Note that the host will create a new event processor and a new
 * event context for every incoming event, then bind them together and eventually aks the processor to process the event.
 *
 * <p>Basically, this is a connection of a connector, an event bound to a host context to be able to
 * process the event.
 */
public interface IEventProcessor {

  /**
   * The method invoked by the XYZ-Hub directly (embedded) or indirectly, when running in an HTTP vertx or as AWS lambda.
   *
   * @param event the event context to process.
   * @return the response to send.
   */
  @NotNull XyzResponse<?> processEvent(@NotNull Event<?> event);
}