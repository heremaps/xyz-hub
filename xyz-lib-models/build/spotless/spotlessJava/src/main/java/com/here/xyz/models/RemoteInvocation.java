package com.here.xyz.models;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.here.xyz.INaksha;
import com.here.xyz.models.hub.plugins.Connector;
import com.here.xyz.models.hub.plugins.EventHandler;
import com.here.xyz.models.hub.plugins.Storage;
import com.here.xyz.models.payload.Event;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Remote invocation object. */
@AvailableSince(INaksha.v2_0)
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "RemoteInvocation")
public class RemoteInvocation extends Payload {

  @AvailableSince(INaksha.v2_0)
  public static final String EVENT_HANDLER = "eventHandler";

  @AvailableSince(INaksha.v2_0)
  public static final String EVENT = "event";

  @AvailableSince(INaksha.v2_0)
  public static final String STORAGE = "storage";

  /**
   * Creates a new remote invocation.
   *
   * @param eventHandler the connector invoked.
   * @param event the event.
   */
  @JsonCreator
  @AvailableSince(INaksha.v2_0)
  public RemoteInvocation(
      @JsonProperty(EVENT) @NotNull EventHandler eventHandler,
      @JsonProperty(EVENT) @NotNull Event event) {
    this.eventHandler = eventHandler;
    if (eventHandler instanceof Connector connector) {
      this.storage = INaksha.get().getStorageById(connector.storageId);
    }
    this.event = event;
  }

  /** The connector invoked. */
  @AvailableSince(INaksha.v2_0)
  @JsonProperty(EVENT_HANDLER)
  public @NotNull EventHandler eventHandler;

  /** If the {@link #eventHandler} is an {@link Connector}, then the storage of the connector. */
  @AvailableSince(INaksha.v2_0)
  @JsonProperty(STORAGE)
  public @Nullable Storage storage;

  /** The event. */
  @AvailableSince(INaksha.v2_0)
  @JsonProperty(EVENT)
  public @NotNull Event event;
}
