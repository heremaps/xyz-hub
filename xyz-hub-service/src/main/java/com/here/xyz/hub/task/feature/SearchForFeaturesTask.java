package com.here.xyz.hub.task.feature;

import com.here.xyz.events.feature.SearchForFeaturesEvent;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SearchForFeaturesTask extends AbstractSearchForFeaturesTask<SearchForFeaturesEvent> {

  public SearchForFeaturesTask(@Nullable String streamId) {
    super(streamId);
  }

  @Override
  public @NotNull SearchForFeaturesEvent createEvent() {
    return new SearchForFeaturesEvent();
  }
}