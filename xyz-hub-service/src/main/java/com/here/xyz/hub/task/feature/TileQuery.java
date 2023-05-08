package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.GetFeaturesByTileEvent;
import com.here.xyz.hub.auth.FeatureAuthorization;
import com.here.xyz.hub.rest.ApiResponseType;
import com.here.xyz.hub.task.NakshaTask;
import com.here.xyz.hub.task.TaskPipeline;
import io.vertx.ext.web.RoutingContext;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;

public class TileQuery extends ReadQuery<GetFeaturesByTileEvent> {

  /**
   * A local copy of some transformation-relevant properties from the event object. NOTE: The event object is not in memory in the
   * response-phase anymore.
   *
   * @see NakshaTask#consumeEvent()
   */
  final @NotNull TransformationContext transformationContext;

  public TileQuery(@NotNull GetFeaturesByTileEvent event, @NotNull RoutingContext context, ApiResponseType apiResponseTypeType) {
    super(event, context, apiResponseTypeType);
    transformationContext = new TransformationContext(event.getX(), event.getY(), event.getLevel(), event.getMargin());
  }

  @Nonnull
  @Nonnull
  @Override
  public TaskPipeline<com.here.xyz.hub.task.feature.TileQuery> initPipeline() {
    return TaskPipeline.create(this)
        .then(FeatureTaskHandler::resolveSpace)
        .then(FeatureAuthorization::authorize)
        .then(FeatureTaskHandler::validate)
        .then(FeatureTaskHandler::readCache)
        .then(FeatureTaskHandler::invoke)
        .then(FeatureTaskHandler::transformResponse)
        .then(FeatureTaskHandler::writeCache);
  }

  static class TransformationContext {

    TransformationContext(int x, int y, int level, int margin) {
      this.x = x;
      this.y = y;
      this.level = level;
      this.margin = margin;
    }

    int x;
    int y;
    int level;
    int margin;
  }
}
