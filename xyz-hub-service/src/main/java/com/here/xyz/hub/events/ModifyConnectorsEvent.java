package com.here.xyz.hub.events;

import com.here.naksha.lib.core.models.hub.Connector;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Pseudo-event to return a connector by ID.
 */
public class ModifyConnectorsEvent extends AbstractConnectorEvent {

  public ModifyConnectorsEvent() {
    this.ids = new ArrayList<>();
  }

  public @Nullable List<@NotNull Connector> insertFeatures;
  public @Nullable List<@NotNull Connector> updateFeatures;
  public @Nullable List<@NotNull Connector> upsertFeatures;
  public @Nullable Map<@NotNull String, @Nullable String> deleteFeatures;

}