package com.here.naksha.lib.core.extension.messages;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.hub.plugins.Connector;
import com.here.naksha.lib.core.models.payload.Event;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * Send from Naksha, when the extension should process an event.
 */
@AvailableSince(INaksha.v2_0_3)
@JsonTypeName(value = "naksha.ext.rpc.v1.processEvent")
public class ProcessEvent extends ExtensionMessage {

  @AvailableSince(INaksha.v2_0_3)
  public static final String CONNECTOR = "connector";

  @AvailableSince(INaksha.v2_0_3)
  public static final String EVENT = "event";

  @JsonCreator
  @AvailableSince(INaksha.v2_0_3)
  public ProcessEvent(
      @JsonProperty(CONNECTOR) @NotNull Connector connector, @JsonProperty(EVENT) @NotNull Event event) {
    this.connector = connector;
    this.event = event;
  }

  @AvailableSince(INaksha.v2_0_3)
  @JsonProperty(CONNECTOR)
  public final @NotNull Connector connector;

  @AvailableSince(INaksha.v2_0_3)
  @JsonProperty(EVENT)
  public final @NotNull Event event;
}
