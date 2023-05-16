package com.here.naksha.activitylog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.here.mapcreator.ext.naksha.PsqlConfig;
import com.here.mapcreator.ext.naksha.PsqlConfigBuilder;
import com.here.xyz.EventHandler;
import com.here.xyz.IEventContext;
import com.here.xyz.XyzSerializable;
import com.here.xyz.events.Event;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.namespaces.XyzActivityLog;
import com.here.xyz.models.geojson.implementation.namespaces.Original;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import com.flipkart.zjsonpatch.JsonDiff;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.here.xyz.models.hub.Connector;
import com.here.xyz.responses.XyzError;
import com.here.xyz.responses.XyzResponse;
import java.util.List;
import java.util.Map;

import com.here.xyz.util.IoHelp;
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


  protected void toActivityLogFormat(@NotNull Feature feature, @Nullable Feature oldFeature) {
    final XyzActivityLog xyzActivityLog = new XyzActivityLog();
    final Original original = new Original();
    final ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNodeFeature = mapper.createObjectNode();
    JsonNode jsonNodeOldFeature = mapper.createObjectNode();
    JsonNode jsonDiff = mapper.createObjectNode();
    if(feature.getProperties() != null && feature.getProperties().getXyzNamespace() != null) {
      original.setPuuid(feature.getProperties().getXyzNamespace().getPuuid());
      original.setMuuid(feature.getProperties().getXyzNamespace().getMuuid());
      original.setUpdatedAt(feature.getProperties().getXyzNamespace().getUpdatedAt());
      original.setCreatedAt(feature.getProperties().getXyzNamespace().getCreatedAt());
      original.setSpace(feature.getProperties().getXyzNamespace().getSpace());
      xyzActivityLog.setAction(feature.getProperties().getXyzNamespace().getAction());
      feature.setId(feature.getProperties().getXyzNamespace().getUuid());
    }
    xyzActivityLog.setOriginal(original);
    xyzActivityLog.setId(feature.getId());
    if(feature.getProperties() != null) {
      feature.getProperties().setXyzActivityLog(xyzActivityLog);
    }
    if(feature.getProperties() != null && feature.getProperties().getXyzActivityLog() != null){
      try {
        jsonNodeFeature = mapper.readTree(XyzSerializable.serialize(feature));
        jsonNodeOldFeature = mapper.readTree(XyzSerializable.serialize(oldFeature));
      } catch (JsonProcessingException e) {
        e.printStackTrace();
      }
      if (jsonNodeFeature != null && jsonNodeOldFeature != null){
        jsonDiff = JsonDiff.asJson(jsonNodeFeature, jsonNodeOldFeature);
      }
      feature.getProperties().getXyzActivityLog().setDiff(jsonDiff);
    }
    //InvalidatedAt can be ignored for now.
  }

  protected static void fromActivityLogFormat(@NotNull Feature activityLogFeature) {
    final XyzActivityLog xyzActivityLog = activityLogFeature.getProperties().getXyzActivityLog();
    final XyzNamespace xyzNamespace = activityLogFeature.getProperties().getXyzNamespace();
    if (xyzActivityLog != null) {
      activityLogFeature.setId(xyzActivityLog.getId());
      if(xyzNamespace != null && xyzActivityLog.getOriginal() != null){
        xyzNamespace.setMuuid(xyzActivityLog.getOriginal().getMuuid());
        xyzNamespace.setPuuid(xyzActivityLog.getOriginal().getPuuid());
        xyzNamespace.setSpace(xyzActivityLog.getOriginal().getSpace());
        xyzNamespace.setUpdatedAt(xyzActivityLog.getOriginal().getUpdatedAt());
        xyzNamespace.setCreatedAt(xyzActivityLog.getOriginal().getCreatedAt());
        //InvalidatedAt can be ignored for now.
      }
      activityLogFeature.getProperties().removeActivityLog();
    }
  }

  protected void FetchActivityLogs() {
    final PsqlConfig config = new PsqlConfigBuilder().withSchema("abc").withDb("adbfg").build();

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
