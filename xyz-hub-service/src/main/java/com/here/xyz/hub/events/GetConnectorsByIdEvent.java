package com.here.xyz.hub.events;

import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * Pseudo-event to return a connector by id.
 */
public class GetConnectorsByIdEvent extends AbstractConnectorEvent {

  public List<@NotNull String> ids;
}