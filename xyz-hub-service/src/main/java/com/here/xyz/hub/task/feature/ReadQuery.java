package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.QueryEvent;
import com.here.xyz.hub.rest.ApiResponseType;
import io.vertx.ext.web.RoutingContext;
import org.jetbrains.annotations.NotNull;

abstract class ReadQuery<EVENT extends QueryEvent, TASK extends FeatureTask<EVENT, TASK>> extends FeatureTask<EVENT, TASK> {

  ReadQuery(@NotNull EVENT event, @NotNull RoutingContext context, ApiResponseType apiResponseTypeType) {
    super(event, context, apiResponseTypeType);
  }

  boolean hasPropertyQuery() {
    return getEvent().getPropertiesQuery() != null;
  }
}
