package com.here.naksha.activitylog;

import com.here.xyz.EventHandler;
import com.here.xyz.IEventContext;
import com.here.xyz.events.Event;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.namespaces.XyzActivityLog;
import com.here.xyz.models.geojson.implementation.Original;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The activity log compatibility handler. Can be used as pre- and post-processor.
 */
public class ActivityLogHandler extends EventHandler {
  public static final String ID = "xyz-hub:activity-log";

  /**
   * Creates a new activity log handler.
   *
   * @param connector The connector configuration.
   * @throws XyzErrorException If any error occurred.
   */
  public ActivityLogHandler(@NotNull Connector connector) throws XyzErrorException {
    super(connector);
    try {
      this.params = new ActivityLogHandlerParams(connector.getParams());
    } catch (Exception e) {
      throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, e.getMessage());
    }
  }

  final @NotNull ActivityLogHandlerParams params;


  protected void toActivityLogFormat(@NotNull Feature feature, @Nullable Feature oldState) {
    final XyzActivityLog xyzActivityLog = new XyzActivityLog();
    final Original original = new Original();
    if(feature.getProperties() != null && feature.getProperties().getXyzNamespace() != null) {
      // TODO: modify feature like:
      //       - Create namespace "@ns:com:here:xyz:log"
      //       - Copy "id" into "@ns:com:here:xyz:log"
      //       - Create "original" sub in "@ns:com:here:xyz:log"
      //       - Copy "createdAt", "updatedAt", "muuid", "puuid" and "space" into "@ns:com:here:xyz:log.original"
      //       - Copy "uuid" from "@ns:com:here:xyz" into root "id"
      //       - Set "@ns:com:here:xyz:log.invalidatedAt", no idea what it does?
      //       - Set "@ns:com:here:xyz:log.action" to "SAVE, "UPDATE", "DELETE"
      //       - Set "@ns:com:here:xyz:log.diff" to the diff between oldState and new feature state.
      original.setPuuid(feature.getProperties().getXyzNamespace().getPuuid());
      original.setMuuid(feature.getProperties().getXyzNamespace().getMuuid());
      original.setUpdatedAt(feature.getProperties().getXyzNamespace().getUpdatedAt());
      original.setSpace(feature.getProperties().getXyzNamespace().getSpace());
      xyzActivityLog.setOrigin(original);
      xyzActivityLog.setId(feature.getId());
      xyzActivityLog.setAction(feature.getProperties().getXyzNamespace().getAction());
      feature.setId(feature.getProperties().getXyzNamespace().getUuid());
      feature.getProperties().setXyzActivityLog(xyzActivityLog);
      //Need to add created at
      //Add Invalidated At
      //Add Diff
    }
    /*
      "original": {
        "muuid": "d8c3afc6-fbde-4542-827b-8ed7f038f199",
        "puuid": "d8c3afc6-fbde-4542-827b-8ed7f038f199",
        "space": "qYlBmcq8",
        "createdAt": 1582715742239,
        "updatedAt": 1582715746638,
        "_inputPosition": 0
      }
     */
  }

  protected void fromActivityLogFormat(@NotNull Feature activityLogFeature) {
    // TODO:
    //     - Remove "@ns:com:here:xyz:log"
    //     - Change "id" back to saved id
    //     - Copy values from "@ns:com:here:xyz:log.original" into "@ns:com:here:xyz"
    //     - What is "invalidatedAt" ?
    final XyzActivityLog xyzActivityLog = activityLogFeature.getProperties().getXyzActivityLog();
    final XyzNamespace xyzNamespace = activityLogFeature.getProperties().getXyzNamespace();
    if (xyzActivityLog != null) {
      activityLogFeature.setId(xyzActivityLog.getId());
      if(xyzNamespace != null && xyzActivityLog.getOriginal() != null){
        xyzNamespace.setMuuid(xyzActivityLog.getOriginal().getMuuid());
        xyzNamespace.setPuuid(xyzActivityLog.getOriginal().getPuuid());
        xyzNamespace.setSpace(xyzActivityLog.getOriginal().getSpace());
        xyzNamespace.setUpdatedAt(xyzActivityLog.getOriginal().getUpdatedAt());
        //Also add created At
      }
      activityLogFeature.getProperties().removeActivityLog();
    }
  }

  @Override
  public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) throws XyzErrorException {
    final Event event = eventContext.getEvent();
    final Map<@NotNull String, Object> spaceParams = event.getParams();
    // TODO: Maybe allow overriding of some parameters per space?
    // TODO: If necessary, perform pre-processing.
    // event.addOldFeatures() <-- we need to see
    XyzResponse response = eventContext.sendUpstream(event);
    if (response instanceof FeatureCollection collection) {
      // TODO: Post-process the features so that they match the new stuff.
      final List<Feature> oldFeatures = collection.getOldFeatures();
      final List<@NotNull Feature> readFeatures = collection.getFeatures();
      for (int i =0; i < readFeatures.size(); i++) {
        // TODO: Improve
        final Feature feature = readFeatures.get(i);
        final Feature oldFeature = oldFeatures.get(i);
        toActivityLogFormat(feature, oldFeature);
      }
    }
    return response;
  }
}
