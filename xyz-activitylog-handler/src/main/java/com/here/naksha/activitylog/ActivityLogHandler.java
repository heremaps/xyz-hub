package com.here.naksha.activitylog;

import com.here.xyz.IEventContext;
import com.here.xyz.IEventHandler;
import com.here.xyz.events.Event;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.responses.XyzResponse;
import org.jetbrains.annotations.NotNull;

/**
 * The activity log compatibility handler. Can be used as pre- and post-processor.
 */
public class ActivityLogHandler implements IEventHandler {

  /**
   * Creates a new activity log handler.
   * @param connector The connector configuration.
   */
  public ActivityLogHandler(@NotNull Connector connector) {
    this.params = new ActivityLogParams(connector.params);
  }

  final @NotNull ActivityLogParams params;

  @Override
  public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
    final Event event = eventContext.event();
    // TODO: If necessary, perform pre-processing.
    XyzResponse response = eventContext.sendUpstream();
    if (response instanceof FeatureCollection collection) {
      // TODO: Post-process the features so that they match the new stuff.
    }
    return response;
  }
}
