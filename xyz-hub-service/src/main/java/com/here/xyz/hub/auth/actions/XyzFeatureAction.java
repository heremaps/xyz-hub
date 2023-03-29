package com.here.xyz.hub.auth.actions;

import org.jetbrains.annotations.NotNull;

/**
 * Access to features in a space. All resource properties filled from the space, not from the feature itself.
 */
public enum XyzFeatureAction {
  READ_FEATURES("readFeatures"),
  CREATE_FEATURES("createFeatures"),
  UPDATE_FEATURES("updateFeatures"),
  DELETE_FEATURES("deleteFeatures");

  XyzFeatureAction(@NotNull String name) {
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