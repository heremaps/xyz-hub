package com.here.xyz.models.payload.events.tweaks;


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonValue;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Delivery of simplified geometries. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "simplification")
public class TweaksSimplification extends Tweaks {

    /** The algorithm. */
    @JsonProperty
    public @NotNull Algorithm algorithm = Algorithm.DEFAULT;

    public enum Algorithm {
        DEFAULT("default", null),
        GRID("grid", "ST_SnapToGrid"),
        GRID_BY_TILE_LEVEL("gridbytilelevel", null),
        SIMPLIFIED_KEEP_TOPOLOGY("simplifiedkeeptopology", "ftm_SimplifyPreserveTopology"),
        SIMPLIFIED("simplified", "ftm_Simplify"),
        MERGE("merge", null),
        LINE_MERGE("linemerge", null);

        Algorithm(@NotNull String text, @Nullable String pgisAlgorithm) {
            this.text = text;
            this.pgisAlgorithm = pgisAlgorithm;
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

        /** The textual representation. */
        public final @NotNull String text;

        // TODO: What does this mean?
        public final @Nullable String pgisAlgorithm;

        @JsonValue
        @Override
        public @NotNull String toString() {
            return text;
        }
    }
}
