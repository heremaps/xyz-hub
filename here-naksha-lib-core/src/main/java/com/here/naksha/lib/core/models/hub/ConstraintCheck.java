package com.here.naksha.lib.core.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

/** A condition to check. */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "Check")
public class ConstraintCheck extends Constraint {

  /** The condition to apply. */
  public enum Test {
    /** If the property is not null; ignores {@link #value}. */
    NOT_NULL,

    /** If the value of the property is unique; ignores {@link #value}. */
    UNIQUE,

    /** If the value of the property is greater than the {@link #value}. */
    GT,

    /** If the value of the property is greater than or equal to the {@link #value}. */
    GTE,

    /** If the value of the property is equal to the {@link #value}. */
    EQ,

    /** If the value of the property is less than the {@link #value}. */
    LT,

    /** If the value of the property is less than or equal to the {@link #value}. */
    LTE,

    /** If the length of the property is more than or equal to the defined {@link #value}. */
    MIN_LEN,

    /** If the length of the property is less than or equal to the defined {@link #value}. */
    MAX_LEN,

    /** If the value matches the given regular expression, given in the {@link #value}. */
    MATCHES
  }

  /** The check to perform. */
  @JsonProperty
  public Test test;

  /** The JSON path to the property to check. */
  @JsonProperty
  public String path;

  /** The optional value for the check. */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public Object value;
}
