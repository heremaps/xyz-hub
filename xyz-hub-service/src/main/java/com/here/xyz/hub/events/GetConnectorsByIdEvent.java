package com.here.xyz.hub.events;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.NotNull;

public class GetConnectorsByIdEvent extends AbstractConnectorEvent {

  public GetConnectorsByIdEvent() {
    this.ids = new ArrayList<>();
  }

  public @NotNull List<@NotNull String> ids;

  public @NotNull GetConnectorsByIdEvent add(@NotNull String id) {
    ids.add(id);
    return this;
  }

  public @NotNull GetConnectorsByIdEvent addAll(@NotNull String... id) {
    Collections.addAll(this.ids, id);
    return this;
  }
}