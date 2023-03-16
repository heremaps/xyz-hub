package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.here.xyz.models.geojson.implementation.XyzNamespace;
import java.util.List;
import java.util.UUID;
import org.jetbrains.annotations.NotNull;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction extends TransactionElement {

  /**
   * A list of transaction elements.
   */
  @JsonProperty
  public List<@NotNull TransactionElement> elements;
}
