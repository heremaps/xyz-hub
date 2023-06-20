package com.here.naksha.lib.core.models.hub;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.List;
import org.jetbrains.annotations.NotNull;

@JsonTypeName(value = "One")
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConstraintOne {

    /** The constraints of which at least one need to hold true (OR). */
    @JsonProperty
    public List<@NotNull Constraint> of;
}
