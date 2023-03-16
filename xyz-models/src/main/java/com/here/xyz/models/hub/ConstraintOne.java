package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View;
import java.util.List;
import org.jetbrains.annotations.NotNull;

@JsonTypeName(value = "One")
@JsonIgnoreProperties(ignoreUnknown = true)
public class ConstraintOne {

  /**
   * The constraints of which at least one need to hold true (OR).
   */
  @JsonProperty
  @JsonView(View.All.class)
  public List<@NotNull Constraint> of;
}
