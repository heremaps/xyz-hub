package com.here.xyz.util.diff;

import org.jetbrains.annotations.Nullable;

public enum ConflictResolution {
  ERROR,
  RETAIN,
  REPLACE;

  public static @Nullable ConflictResolution of(@Nullable String value) {
    if (value != null) {
      for (final ConflictResolution cr : ConflictResolution.values()) {
        if (cr.name().equalsIgnoreCase(value)) {
          return cr;
        }
      }
    }
    return null;
  }
}
