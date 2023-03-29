package com.here.xyz.hub.auth.actions;

import org.jetbrains.annotations.NotNull;

/**
 * Access to connectors.
 */
public enum XyzConnectorAction {
  /**
   * Allows to use connectors when creating or modifying a space. Not necessary to read features from a space using a connector.
   */
  ACCESS_CONNECTORS("accessConnectors"),

  /**
   * Allows to create, modify or delete connectors. Does not include the right to use the connector in a space!
   */
  MANAGE_CONNECTORS("manageConnectors");

  XyzConnectorAction(@NotNull String name) {
    this.name = name;
  }

  /**
   * The action name.
   */
  public final @NotNull String name;

  @Override
  public @NotNull String toString() {
    return name;
  }
}