package com.here.naksha.lib.core.storage;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Delete the entity with the given identifier in the given state. The delete will fail, if the entity is not in the desired state.
 *
 * @param id   the identifier of the feature to delete.
 * @param uuid the UUID of the state to delete, {@code null}, if any state is acceptable.
 */
public record DeleteOp(@NotNull String id, @Nullable String uuid) {

  public DeleteOp(@NotNull String id) {
    this(id, null);
  }
}
