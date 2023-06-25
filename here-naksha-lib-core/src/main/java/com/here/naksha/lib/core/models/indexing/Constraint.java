package com.here.naksha.lib.core.models.indexing;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

/** Base class of all possible constraints that can be combined. */
@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = ConstraintCheck.class),
  @JsonSubTypes.Type(value = ConstraintAll.class),
  @JsonSubTypes.Type(value = ConstraintOne.class),
  @JsonSubTypes.Type(value = ConstraintNot.class)
})
public class Constraint {}
