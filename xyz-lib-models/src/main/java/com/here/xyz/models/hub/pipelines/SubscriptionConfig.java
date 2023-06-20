package com.here.xyz.models.hub.pipelines;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.view.View;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SubscriptionConfig {

  /**
   * The type of the subscription.
   */
  @JsonProperty
  private SubscriptionType type;

  @JsonProperty
  @JsonView(View.Protected.class)
  private Map<@NotNull String, @Nullable Object> params;

  public SubscriptionType getType() {
    return type;
  }

  public void setType(SubscriptionType type) {
    this.type = type;
  }

  public @NotNull SubscriptionConfig withType(SubscriptionType type) {
    this.type = type;
    return this;
  }

  public Map<@NotNull String, @Nullable Object> getParams() {
    return params;
  }

  public void setParams(Map<@NotNull String, @Nullable Object> params) {
    this.params = params;
  }

  public @NotNull SubscriptionConfig withParams(Map<@NotNull String, @Nullable Object> params) {
    this.params = params;
    return this;
  }

  public enum SubscriptionType {
    PER_FEATURE, PER_TRANSACTION, CONTENT_CHANGE
  }
}
