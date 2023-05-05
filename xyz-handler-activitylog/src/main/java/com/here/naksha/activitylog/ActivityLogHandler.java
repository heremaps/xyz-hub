package com.here.naksha.activitylog;

import com.here.xyz.EventHandler;
import com.here.xyz.IEventContext;
import com.here.xyz.events.Event;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The activity log compatibility handler. Can be used as pre- and post-processor.
 */
public class ActivityLogHandler extends EventHandler {

  /**
   * Creates a new activity log handler.
   *
   * @param connector The connector configuration.
   * @throws XyzErrorException If any error occurred.
   */
  public ActivityLogHandler(@NotNull Connector connector) throws XyzErrorException {
    super(connector);
    try {
      this.params = new ActivityLogParams(connector.params);
    } catch (Exception e) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, e.getMessage());
    }
  }

  final @NotNull ActivityLogParams params;


  protected @NotNull Feature toActivityLogFormat(@NotNull Feature feature, @Nullable Feature oldState) {
    // TODO: modify feature like:
    //       - Create namespace "@ns:com:here:xyz:log"
    //       - Copy "id" into "@ns:com:here:xyz:log"
    //       - Create "original" sub in "@ns:com:here:xyz:log"
    //       - Copy "createdAt", "updatedAt", "muuid", "puuid" and "space" into "@ns:com:here:xyz:log.original"
    //       - Copy "uuid" from "@ns:com:here:xyz" into root "id"
    //       - Set "@ns:com:here:xyz:log.invalidatedAt", no idea what it does?
    //       - Set "@ns:com:here:xyz:log.action" to "SAVE, "UPDATE", "DELETE"
    //       - Set "@ns:com:here:xyz:log.diff" to the diff between oldState and new feature state.
    /*
      "original": {
        "muuid": "d8c3afc6-fbde-4542-827b-8ed7f038f199",
        "puuid": "d8c3afc6-fbde-4542-827b-8ed7f038f199",
        "space": "qYlBmcq8",
        "createdAt": 1582715742239,
        "updatedAt": 1582715746638,
        "_inputPosition": 0
      },
     */
    return feature;
  }

  protected @NotNull Feature fromActivityLogFormat(@NotNull Feature activityLogFeature) {
    // TODO:
    //     - Remove "@ns:com:here:xyz:log"
    //     - Change "id" back to saved id
    //     - Copy values from "@ns:com:here:xyz:log.original" into "@ns:com:here:xyz"
    //     - What is "invalidatedAt" ?
    return activityLogFeature;
  }

  @Override
  public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
    final Event event = eventContext.event();
    // TODO: If necessary, perform pre-processing.
    XyzResponse response = eventContext.sendUpstream();
    if (response instanceof FeatureCollection collection) {
      // TODO: Post-process the features so that they match the new stuff.
      final List<@NotNull Feature> features = collection.getFeatures();
      for (final @NotNull Feature feature : features) {
        // TODO:
      }
    }
    return response;
  }
}
