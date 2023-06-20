package com.here.naksha.handler.activitylog;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.flipkart.zjsonpatch.JsonDiff;
import com.here.xyz.IEventContext;
import com.here.xyz.IEventHandler;
import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.geojson.implementation.Feature;
import com.here.xyz.models.geojson.implementation.FeatureCollection;
import com.here.xyz.models.geojson.implementation.namespaces.Original;
import com.here.xyz.models.geojson.implementation.namespaces.XyzActivityLog;
import com.here.xyz.models.geojson.implementation.namespaces.XyzNamespace;
import com.here.xyz.models.hub.plugins.EventHandler;
import com.here.xyz.models.payload.Event;
import com.here.xyz.models.payload.XyzResponse;
import com.here.xyz.models.payload.responses.XyzError;
import com.here.xyz.util.json.JsonSerializable;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** The activity log compatibility handler. Can be used as pre- and post-processor. */
public class ActivityLogHandler implements IEventHandler {

    /**
     * Creates a new activity log handler.
     *
     * @param eventHandler the event-handler configuration.
     * @throws XyzErrorException If any error occurred.
     */
    public ActivityLogHandler(@NotNull EventHandler eventHandler) throws XyzErrorException {
        try {
            this.params = new ActivityLogHandlerParams(eventHandler.getProperties());
        } catch (Exception e) {
            throw new XyzErrorException(XyzError.ILLEGAL_ARGUMENT, e.getMessage());
        }
    }

    final @NotNull ActivityLogHandlerParams params;

    protected static void toActivityLogFormat(@NotNull Feature feature, @Nullable Feature oldFeature) {
        final XyzActivityLog xyzActivityLog = new XyzActivityLog();
        final Original original = new Original();
        final ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNodeFeature = mapper.createObjectNode();
        JsonNode jsonNodeOldFeature = mapper.createObjectNode();
        JsonNode jsonDiff = mapper.createObjectNode();
        if (feature.getProperties() != null && feature.getProperties().getXyzNamespace() != null) {
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
        if (feature.getProperties() != null) {
            feature.getProperties().setXyzActivityLog(xyzActivityLog);
        }
        if (feature.getProperties() != null && feature.getProperties().getXyzActivityLog() != null) {
            try {
                jsonNodeFeature = mapper.readTree(JsonSerializable.serialize(feature));
                jsonNodeOldFeature = mapper.readTree(JsonSerializable.serialize(oldFeature));
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            if (jsonNodeFeature != null && jsonNodeOldFeature != null) {
                jsonDiff = JsonDiff.asJson(jsonNodeFeature, jsonNodeOldFeature);
            }
            feature.getProperties().getXyzActivityLog().setDiff(jsonDiff);
        }
        // InvalidatedAt can be ignored for now.
    }

    protected static void fromActivityLogFormat(@NotNull Feature activityLogFeature) {
        final XyzActivityLog xyzActivityLog = activityLogFeature.getProperties().getXyzActivityLog();
        final XyzNamespace xyzNamespace = activityLogFeature.getProperties().getXyzNamespace();
        if (xyzNamespace != null) {
            xyzNamespace.setUuid(activityLogFeature.getId());
        }
        if (xyzActivityLog != null) {
            if (xyzActivityLog.getOriginal() != null) {
                if (xyzActivityLog.getOriginal().getPuuid() == null) {
                    xyzNamespace.setAction("CREATE");
                }
                if (xyzActivityLog.getOriginal().getPuuid() != null) {
                    xyzNamespace.setAction("UPDATE");
                }
            }
            if (xyzActivityLog.getAction() == "DELETE" && xyzNamespace != null) {
                xyzNamespace.setAction("DELETE");
            }
            activityLogFeature.setId(xyzActivityLog.getId());
            if (xyzNamespace != null && xyzActivityLog.getOriginal() != null) {
                xyzNamespace.setMuuid(xyzActivityLog.getOriginal().getMuuid());
                xyzNamespace.setPuuid(xyzActivityLog.getOriginal().getPuuid());
                xyzNamespace.setSpace(xyzActivityLog.getOriginal().getSpace());
                xyzNamespace.setUpdatedAt(xyzActivityLog.getOriginal().getUpdatedAt());
                xyzNamespace.setCreatedAt(xyzActivityLog.getOriginal().getCreatedAt());
                // InvalidatedAt can be ignored for now.
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
            for (int i = 0; i < readFeatures.size(); i++) {
                // TODO: Improve
                final Feature feature = readFeatures.get(i);
                final Feature oldFeature = oldFeatures.get(i);
                toActivityLogFormat(feature, oldFeature);
            }
        }
        return response;
    }
}
