package com.here.xyz.util.modify;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.jetbrains.annotations.Nullable;

public enum IfExists {
  RETAIN,
  ERROR,
  DELETE,
  REPLACE,
  PATCH,
  MERGE;

  @JsonCreator
  public static @Nullable IfExists of(@Nullable String value) {
    if (value != null) {
      for (final IfExists e : IfExists.values()) {
        if (e.name().equalsIgnoreCase(value)) {
          return e;
        }
      }
    }
    return null;
  }
}
