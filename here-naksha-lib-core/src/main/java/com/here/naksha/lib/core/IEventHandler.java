package com.here.naksha.lib.core;

import com.here.naksha.lib.core.exceptions.XyzErrorException;
import com.here.naksha.lib.core.models.payload.Event;
import com.here.naksha.lib.core.models.payload.XyzResponse;
import org.jetbrains.annotations.NotNull;

/**
 * The default interface that must be implemented by all event handler. A handler is code that
 * performs actions with an event bound to an event context provided by the host application. Note
 * that the host will create a new event pipeline for every new event, add all necessary handlers to
 * it and then send the event through the pipeline. The pipeline itself provides the {@link
 * IEventContext} and invokes all handlers in order. Every handler can either consume the event or
 * {@link IEventContext#sendUpstream(Event) send it upstream}.
 */
@FunctionalInterface
public interface IEventHandler {

    /**
     * The method invoked by the XYZ-Hub directly (embedded) or indirectly, when running in an HTTP
     * vertx or as AWS lambda.
     *
     * @param eventContext the event context to process.
     * @return the response to send.
     * @throws XyzErrorException if any error occurred.
     */
    @NotNull
    XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException;
}
