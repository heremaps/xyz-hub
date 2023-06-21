package com.here.naksha.lib.core.models.payload.events;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Sample settings. */
@SuppressWarnings("unused")
@JsonIgnoreProperties(ignoreUnknown = true)
public final class Sampling {

    // Named settings.
    public static final Sampling OFF = new Sampling(1, 1, 0, 100_000, "off");
    public static final Sampling LOW = new Sampling(1, 8, 10, 80_000, "low");
    public static final Sampling LOW_MEDIUM = new Sampling(1, 32, 30, 60_000, "lowmed");
    public static final Sampling MEDIUM = new Sampling(1, 128, 50, 40_000, "med");
    public static final Sampling MEDIUM_HIGH = new Sampling(1, 1024, 75, 20_000, "medhigh");
    public static final Sampling HIGH = new Sampling(1, 4096, 100, 10_000, "high");

    /**
     * Can be used to create a new sample settings.
     *
     * @param numerator The numerator of the ratio.
     * @param denominator The denominator of the ratio.
     * @param strength The sampling strength.
     * @param threshold The sampling threshold.
     */
    public Sampling(int numerator, int denominator, int strength, int threshold) {
        this.numerator = numerator;
        this.denominator = denominator;
        this.strength = strength;
        this.threshold = threshold;
        this.name = "{"
                + "\"numerator\":"
                + numerator
                + ",\n"
                + "\"denominator\":"
                + denominator
                + ",\n"
                + "\"strength\":"
                + strength
                + ",\n"
                + "\"threshold\":"
                + threshold
                + "}";
    }

    /**
     * Create a new name sample.
     *
     * @param numerator The numerator of the ratio.
     * @param denominator The denominator of the ratio.
     * @param strength The sampling strength.
     * @param threshold The sampling threshold.
     * @param name The pre-define name.
     */
    private Sampling(int numerator, int denominator, int strength, int threshold, @NotNull String name) {
        this.numerator = numerator;
        this.denominator = denominator;
        this.strength = strength;
        this.threshold = threshold;
        this.name = name;
        allByText.put(name, this);
    }

    /**
     * Returns the clustering parameter value for the given text.
     *
     * @param text The text.
     * @param alt alt The alternative value to return, when the text does not match to any known
     *     sampling.
     * @return The clustering or the given alternative.
     */
    @JsonCreator
    public static @NotNull Sampling forText(@Nullable String text, @NotNull Sampling alt) {
        if (text == null || text.length() < 3) {
            return alt;
        }
        final Sampling sampling = allByText.get(text);
        return sampling != null ? sampling : alt;
    }

    /** The textual representation. */
    public final @NotNull String name;

    /** The numerator of the sampling. */
    public final int numerator;

    /** The denominator of the sampling. */
    public final int denominator;

    /** The relative sampling strength. */
    // TODO: Improve the documentation!
    public final int strength;

    /** The size of samples (between 10k and 100k). */
    public final int threshold;

    @Override
    public @NotNull String toString() {
        return name;
    }

    private static final ConcurrentHashMap<@NotNull String, @NotNull Sampling> allByText = new ConcurrentHashMap<>();
}
