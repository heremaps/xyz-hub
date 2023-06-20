package com.here.xyz;


import com.here.xyz.exceptions.XyzErrorException;
import com.here.xyz.models.hub.plugins.EventHandler;
import com.here.xyz.models.payload.Event;
import com.here.xyz.models.payload.XyzResponse;
import com.here.xyz.models.payload.events.admin.ModifySubscriptionEvent;
import com.here.xyz.models.payload.events.feature.DeleteFeaturesByTagEvent;
import com.here.xyz.models.payload.events.feature.GetFeaturesByBBoxEvent;
import com.here.xyz.models.payload.events.feature.GetFeaturesByGeometryEvent;
import com.here.xyz.models.payload.events.feature.GetFeaturesByIdEvent;
import com.here.xyz.models.payload.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.models.payload.events.feature.IterateFeaturesEvent;
import com.here.xyz.models.payload.events.feature.LoadFeaturesEvent;
import com.here.xyz.models.payload.events.feature.ModifyFeaturesEvent;
import com.here.xyz.models.payload.events.feature.SearchForFeaturesEvent;
import com.here.xyz.models.payload.events.feature.history.IterateHistoryEvent;
import com.here.xyz.models.payload.events.info.GetHistoryStatisticsEvent;
import com.here.xyz.models.payload.events.info.GetStatisticsEvent;
import com.here.xyz.models.payload.events.info.GetStorageStatisticsEvent;
import com.here.xyz.models.payload.events.info.HealthCheckEvent;
import com.here.xyz.models.payload.events.space.ModifySpaceEvent;
import com.here.xyz.models.payload.responses.ErrorResponse;
import com.here.xyz.models.payload.responses.XyzError;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

/**
 * Default implementation of an extended event handler that allows to only implement handling for
 * supported events, and optionally of some post-processing.
 */
public class ExtendedEventHandler<HANDLER extends EventHandler> implements IExtendedEventHandler {

    public ExtendedEventHandler(@NotNull HANDLER eventHandler) throws XyzErrorException {
        this.eventHandler = eventHandler;
    }

    @Override
    public @NotNull XyzResponse processEvent(@NotNull IEventContext eventContext) {
        return postProcess(IExtendedEventHandler.super.processEvent(eventContext));
    }

    @Override
    public void initialize(@NotNull IEventContext ctx) {
        this.ctx = ctx;
        this.event = ctx.getEvent();
    }

    protected final @NotNull HANDLER eventHandler;
    protected Event event;
    protected IEventContext ctx;

    /**
     * Creates an error response to return.
     *
     * @param error the error type.
     * @param message the error message.
     * @return the generated error response.
     */
    protected @NotNull XyzResponse errorResponse(@NotNull XyzError error, @NotNull CharSequence message) {
        return new ErrorResponse()
                .withStreamId(event.getStreamId())
                .withError(error)
                .withErrorMessage(message.toString());
    }

    /**
     * Can be overridden to post-process responses.
     *
     * @param response the response.
     * @return the post-processed response.
     */
    protected @NotNull XyzResponse postProcess(@NotNull XyzResponse response) {
        return response;
    }

    /**
     * Send the currently processed event upstream towards the storage.
     *
     * @param event the event to send upstream.
     * @return the response returned by the storage and before post-processing.
     */
    protected @NotNull XyzResponse sendUpstream(@NotNull Event event) {
        return ctx.sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processHealthCheckEvent(@NotNull HealthCheckEvent event) {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processGetStatistics(@NotNull GetStatisticsEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processGetHistoryStatisticsEvent(@NotNull GetHistoryStatisticsEvent event)
            throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processGetFeaturesByIdEvent(@NotNull GetFeaturesByIdEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processGetFeaturesByGeometryEvent(@NotNull GetFeaturesByGeometryEvent event)
            throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processGetFeaturesByBBoxEvent(@Nonnull GetFeaturesByBBoxEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processGetFeaturesByTileEvent(@NotNull GetFeaturesByTileEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processIterateFeaturesEvent(@NotNull IterateFeaturesEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processSearchForFeaturesEvent(@NotNull SearchForFeaturesEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processDeleteFeaturesByTagEvent(@NotNull DeleteFeaturesByTagEvent event)
            throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processLoadFeaturesEvent(@NotNull LoadFeaturesEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processModifyFeaturesEvent(@NotNull ModifyFeaturesEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processModifySpaceEvent(@NotNull ModifySpaceEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processModifySubscriptionEvent(@NotNull ModifySubscriptionEvent event)
            throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processIterateHistoryEvent(@NotNull IterateHistoryEvent event) throws Exception {
        return sendUpstream(event);
    }

    @Override
    public @NotNull XyzResponse processGetStorageStatisticsEvent(@NotNull GetStorageStatisticsEvent event)
            throws Exception {
        return sendUpstream(event);
    }
}
