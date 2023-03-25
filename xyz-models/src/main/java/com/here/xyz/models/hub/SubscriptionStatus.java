package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View.All;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubscriptionStatus {

  /**
   * The type of the subscription
   */
  @JsonProperty
  @JsonView(All.class)
  private State state;

  @JsonProperty
  @JsonView(All.class)
  @JsonInclude(Include.NON_NULL)
  private String stateReason;

  public State getState() {
    return state;
  }

  public void setState(State state) {
    this.state = state;
  }

  public SubscriptionStatus withState(State state) {
    this.state = state;
    return this;
  }

  public String getStateReason() {
    return stateReason;
  }

  public void setStateReason(String stateReason) {
    this.stateReason = stateReason;
  }

  public SubscriptionStatus withStateReason(String stateReason) {
    this.stateReason = stateReason;
    return this;
  }

  public enum State {
    ACTIVE, INACTIVE, SUSPENDED, PENDING, AUTH_FAILED
  }
}
