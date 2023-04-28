package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.GetFeaturesByBBoxEvent;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class GetFeaturesByBBoxTask extends AbstractGetFeaturesByBBoxTask<GetFeaturesByBBoxEvent> {

  public GetFeaturesByBBoxTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  public @NotNull GetFeaturesByBBoxEvent createEvent() {
    return new GetFeaturesByBBoxEvent();
  }
}
