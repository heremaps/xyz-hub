package com.here.xyz.events.tweaks;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Delivery of geometry distributed data-samples.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName(value = "ensure")
public class TweaksEnsure extends TweaksSampling {

  /**
   * Use a standard selection of feature properties.
   */
  public boolean defaultSelection;
}