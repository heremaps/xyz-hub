package com.here.naksha.lib.core.models.indexing;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(value = "Not")
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConstraintNot {

  /** The constraints that should be negated. */
  @JsonProperty
  public Constraint of;
}
