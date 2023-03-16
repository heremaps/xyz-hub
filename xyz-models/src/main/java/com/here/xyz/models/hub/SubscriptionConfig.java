package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubscriptionConfig {

  /**
   * The type of the subscription
   */
  private SubscriptionType type;

  private Map<String, Object> params;

  public SubscriptionType getType() {
    return type;
  }

  public void setType(SubscriptionType type) {
    this.type = type;
  }

  public SubscriptionConfig withType(SubscriptionType type) {
    this.type = type;
    return this;
  }

  public Map<String, Object> getParams() {
    return params;
  }

  public void setParams(Map<String, Object> params) {
    this.params = params;
  }

  public SubscriptionConfig withParams(Map<String, Object> params) {
    this.params = params;
    return this;
  }

  public enum SubscriptionType {
    PER_FEATURE, PER_TRANSACTION, CONTENT_CHANGE
  }
}
