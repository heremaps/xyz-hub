package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

/**
 * A virtual wrapper to bind transaction elements together into a root transaction element.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction extends TransactionElement {

  /**
   * A map between space-id and the corresponding transaction element.
   */
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_EMPTY)
  public Map<@NotNull String, @NotNull TransactionElement> transaction;

  /**
   * A map between the commit-id and the corresponding commit message.
   */
  @JsonProperty
  @JsonView(View.All.class)
  @JsonInclude(Include.NON_EMPTY)
  public Map<@NotNull String, @NotNull TransactionCommitMessage> msg;
}
