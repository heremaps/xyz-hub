package com.here.xyz.util.modify;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.jetbrains.annotations.Nullable;

/**
 * The action to perform if a feature does not exist.
 */
public enum IfNotExists {
  RETAIN,
  ERROR,
  CREATE;

  @JsonCreator
  public static @Nullable IfNotExists of(@Nullable String value) {
    if (value != null) {
      for (final IfNotExists e : IfNotExists.values()) {
        if (e.name().equalsIgnoreCase(value)) {
          return e;
        }
      }
    }
    return null;
  }
}
