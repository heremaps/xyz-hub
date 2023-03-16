package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View;

/**
 * The copyright information object.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Copyright {

  /**
   * The copyright label to be displayed by the client.
   */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  @JsonView(View.All.class)
  private String label;

  /**
   * The description text for the label to be displayed by the client.
   */
  @JsonProperty
  @JsonInclude(Include.NON_NULL)
  @JsonView(View.All.class)
  private String alt;

  public String getLabel() {
    return label;
  }

  public void setLabel(final String label) {
    this.label = label;
  }

  public Copyright withLabel(final String label) {
    setLabel(label);
    return this;
  }

  public String getAlt() {
    return alt;
  }

  public void setAlt(final String alt) {
    this.alt = alt;
  }

  public Copyright withAlt(final String alt) {
    setAlt(alt);
    return this;
  }
}
