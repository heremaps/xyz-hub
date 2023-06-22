package com.here.naksha.lib.core.extension.messages;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.naksha.lib.core.INaksha;
import com.here.naksha.lib.core.models.payload.Event;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * Send by the extension when Naksha should forward an event through the event-pipeline. Naksha will respond with a {@link }
 */
@AvailableSince(INaksha.v2_0_3)
@JsonTypeName(value = "naksha.ext.rpc.v1.sendUpdate")
public class SendUpstream extends ExtensionMessage {

  public static final String EVENT = "event";

  public SendUpstream(@JsonProperty(EVENT) @NotNull Event event) {
    this.event = event;
  }

  @AvailableSince(INaksha.v2_0_3)
  @JsonProperty(EVENT)
  public final @NotNull Event event;
}
