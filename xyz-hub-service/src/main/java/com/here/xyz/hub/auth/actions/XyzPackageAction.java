package com.here.xyz.hub.auth.actions;

import org.jetbrains.annotations.NotNull;

/**
 * Access to packages.
 */
public enum XyzPackageAction {
  /**
   * Needed to add or remove spaces or connectors from/to packages.
   */
  MANAGE_PACKAGES("managePackages");

  XyzPackageAction(@NotNull String name) {
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
