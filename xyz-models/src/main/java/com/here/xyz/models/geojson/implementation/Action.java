package com.here.xyz.models.geojson.implementation;

import org.checkerframework.checker.nullness.qual.AssertNonNullIfNonNull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The actions that are supported by Naksha.
 */
public final class Action {

  private Action(@NotNull String action) {
    value = action;
  }

  private final @NotNull String value;

  /**
   * Returns an action instance for the given value.
   *
   * @param value the value to check.
   * @return the action instance matching or {@code null}.
   */
  public static @Nullable Action get(@Nullable String value) {
    if (value != null) {
      // Note:
      if (CREATE.value.equals(value)) {
        return CREATE;
      }
      if (UPDATE.value.equals(value)) {
        return UPDATE;
      }
      if (DELETE.value.equals(value)) {
        return DELETE;
      }
    }
    return null;
  }

  /**
   * The feature has just been created, the {@link XyzNamespace#getVersion() version} will be {@code 1}.
   */
  public static final Action CREATE = new Action("CREATE");

  /**
   * The feature has been updated, the {@link XyzNamespace#getVersion() version} will be greater than {@code 1}.
   */
  public static final Action UPDATE = new Action("UPDATE");

  /**
   * The feature has been deleted, the {@link XyzNamespace#getVersion() version} will be greater than {@code 1}. No other state with a
   * higher version should be possible.
   */
  public static final Action DELETE = new Action("DELETE");

  @Override
  public int hashCode() {
    return value.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return this == o;
  }

  /**
   * Tests if the given text represents this action.
   * @param text the text to test.
   * @return true if the given text represents this action; false otherwise.
   */
  public boolean equals(@Nullable String text) {
    return value.equals(text);
  }

  public @NotNull String toString() {
    return value;
  }
}
