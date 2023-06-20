package com.here.xyz.models.payload.events.tweaks;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.here.xyz.models.Typed;
import com.here.xyz.models.payload.events.Sampling;
import org.jetbrains.annotations.NotNull;

/**
 * Providing this parameter only a subset of the data will be returned. This can be used for
 * rendering higher zoom levels.
 */
@JsonSubTypes({
    @JsonSubTypes.Type(value = TweaksSimplification.class),
    @JsonSubTypes.Type(value = TweaksSampling.class, name = "sampling"),
    @JsonSubTypes.Type(value = TweaksEnsure.class, name = "ensure")
})
public abstract class Tweaks implements Typed {

    /**
     * The sampling settings; if any.
     *
     * <p>This is simplified, its called sometimes strength, then sampling and “samplingthreshold”.
     * This is a combination of all, and we now allow to either select a pre-defined name via
     * "&tweaks:sampling" and to override individual values via "&tweaks:sampling:strength". The
     * default selection is always “off”.
     */
    @JsonProperty
    public @NotNull Sampling sampling = Sampling.OFF;
}
