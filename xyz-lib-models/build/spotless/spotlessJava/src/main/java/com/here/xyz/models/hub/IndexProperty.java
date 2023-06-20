package com.here.xyz.models.hub;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class IndexProperty {

  /** The JSON path to the property to index. */
  @JsonProperty public String path;

  /** If the property should be naturally ordered ascending. */
  @JsonProperty public boolean asc = true;

  /**
   * Optionally decide if {@link null} values should be ordered first or last. If not explicitly
   * defined, automatically decided.
   */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  public Nulls nulls;

  public enum Nulls {
    FIRST,
    LAST
  }
}
