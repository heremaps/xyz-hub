package com.here.xyz.models.hub.pipelines;


import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.INaksha;
import com.here.xyz.models.geojson.implementation.Feature;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * All configurations that represent an event-pipeline component must extend this class. A pipelined
 * component is a component, that acts on events send through an event-pipeline.
 */
@SuppressWarnings("unused")
@AvailableSince(INaksha.v2_0)
public abstract class PipelineComponent extends Feature {

  @AvailableSince(INaksha.v2_0)
  public static final String EVENT_HANDLERS = "eventHandlers";

  /**
   * Create a new empty pipeline.
   *
   * @param id the identifier of this component.
   * @param eventHandlers the list of event handler identifiers to form the event-pipeline.
   */
  @AvailableSince(INaksha.v2_0)
  public PipelineComponent(@NotNull String id, @NotNull List<@NotNull String> eventHandlers) {
    super(id);
    this.eventHandlers = eventHandlers;
  }

  /**
   * Create a new empty pipeline.
   *
   * @param id the ID.
   * @param eventHandlers the list of event-handler identifier of the event handlers that form the
   *     event-pipeline.
   * @param packages the packages this feature is part of.
   */
  @AvailableSince(INaksha.v2_0)
  public PipelineComponent(
      @NotNull String id,
      @NotNull List<@NotNull String> eventHandlers,
      @Nullable List<@NotNull String> packages) {
    super(id);
    setEventHandlers(eventHandlers);
    setPackages(packages);
  }

  /** The list of event-handler identifiers to be added to the event pipeline, in order. */
  @AvailableSince(INaksha.v2_0)
  @JsonProperty(EVENT_HANDLERS)
  public @NotNull List<@NotNull String> eventHandlers;

  /**
   * Returns all event-handler identifiers.
   *
   * @return all event-handler identifiers.
   */
  @AvailableSince(INaksha.v2_0)
  @JsonIgnore
  public @NotNull List<@NotNull String> getEventHandlers() {
    return eventHandlers;
  }

  /**
   * Replace the event-handler identifiers.
   *
   * @param eventHandlers the event-handler identifiers.
   */
  @AvailableSince(INaksha.v2_0)
  public void setEventHandlers(@NotNull List<@NotNull String> eventHandlers) {
    this.eventHandlers = eventHandlers;
  }

  /**
   * Replace the event-handler identifiers.
   *
   * @param eventHandlerIds the new event-handler identifiers.
   */
  @AvailableSince(INaksha.v2_0)
  public void setEventHandlerIds(@NotNull String... eventHandlerIds) {
    this.eventHandlers = new ArrayList<>(eventHandlerIds.length);
    Collections.addAll(this.eventHandlers, eventHandlerIds);
  }
}
