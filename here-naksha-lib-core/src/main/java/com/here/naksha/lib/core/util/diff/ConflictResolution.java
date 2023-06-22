package com.here.naksha.lib.core.util.diff;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.jetbrains.annotations.Nullable;

/** The way conflicts are to be resolved by the modification entry. */
public enum ConflictResolution {
  /**
   * Whenever the current head state is different as the base state, abort the transaction and
   * report an error.
   */
  ERROR,

  /**
   * Whenever the current head state is different as the base state, keep the current head and
   * continue the transaction without error.
   */
  RETAIN,

  /**
   * Always replace the current head state with the given version, no matter if any other change
   * conflicts. In this case, the puuid will refer to the state being replaced.
   */
  REPLACE;

  /**
   * Whenever the current head state is different as the base state, try a <a
   * href="https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge>three-way-merge</a>
   * and if that fails, abort the transaction with an error.
   */
  // MERGE_OR_ERROR,

  /**
   * Whenever the current head state is different as the base state, try a <a
   * href="https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge>three-way-merge</a>
   * and conflicting properties or parts of the feature will be retained (stay unmodified).
   */
  // MERGE_OR_RETAIN,

  /**
   * Whenever the current head state is different as the base state, try a <a
   * href="https://en.wikipedia.org/wiki/Merge_(version_control)#Three-way_merge>three-way-merge</a>
   * and conflicting properties or parts of the feature will be replaced with the new version.
   */
  // MERGE_OR_REPLACE;

  /**
   * Returns the conflict resolution from the given text.
   *
   * @param value The text to parse.
   * @return The found conflict resolution; {@code null} if none matches.
   */
  @JsonCreator
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
