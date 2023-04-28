package com.here.xyz.events.tweaks;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Delivery of geometry distributed data-samples.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "sampling")
public class TweaksSampling extends Tweaks {

  /**
   * The algorithm.
   */
  @JsonProperty
  public Algorithm algorithm;

  public @NotNull Algorithm algorithm() {
    return algorithm != null ? algorithm : Algorithm.GEOMETRY_SIZE;
  }

  public enum Algorithm {
    GEOMETRY_SIZE("geometrysize", false, false),
    DISTRIBUTION("distribution", true, false),
    DISTRIBUTION2("distribution2", true, true);

    Algorithm(@NotNull String text, boolean distribution, boolean distribution2) {
      this.text = text;
      this.distribution = distribution;
      this.distribution2 = distribution;
    }

    @JsonCreator
    public static @Nullable Algorithm forText(@Nullable String text) {
      if (text != null) {
        for (final @NotNull Algorithm algorithm : values()) {
          if (algorithm.text.equalsIgnoreCase(text)) {
            return algorithm;
          }
        }
      }
      return null;
    }

    /**
     * The textual representation.
     */
    public final @NotNull String text;

    /**
     * If the algorithm implies distribution.
     */
    public final boolean distribution;

    /**
     * If the algorithm implies distribution version 2.
     */
    public final boolean distribution2;

    @JsonValue
    @Override
    public @NotNull String toString() {
      return text;
    }
  }

}