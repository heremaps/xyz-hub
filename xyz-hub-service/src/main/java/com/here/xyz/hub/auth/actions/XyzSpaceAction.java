package com.here.xyz.hub.auth.actions;

import org.jetbrains.annotations.NotNull;

/**
 * Access to spaces. Generally you have read access to the basic details of every space you have {@link XyzFeatureAction#READ_FEATURES}
 * right.
 */
public enum XyzSpaceAction {
  /**
   * Allow to create, update or delete spaces.
   */
  MANAGE_SPACES("manageSpaces");

  XyzSpaceAction(@NotNull String name) {
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
