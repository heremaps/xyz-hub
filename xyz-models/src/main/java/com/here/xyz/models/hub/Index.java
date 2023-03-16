package com.here.xyz.models.hub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonView;
import com.here.xyz.View;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * The specification of an index.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Index {

  /**
   * The algorithm to use. The implementing processor will decide if it supports the algorithm.
   *
   * <p>The PostgresQL processor supports the following algorithms (with its recommended targets):
   * <li>{@code btree} for {@code String}, {@code Number} and {@code Boolean}.
   * <li>{@code hash} for {@code String}, {@code Number} and {@code Boolean}.
   * <li>{@code brin} for {@code String}, {@code Number} and {@code Boolean}.
   * <li>{@code gin} for {@code List} and {@code Map}.
   * <li>{@code gin_trigram} for {@code String}.
   *
   * <p>Note that if no algorithm given, the PostgresQL processor will auto-select on and return the selected algorithm in the response.
   */
  @JsonProperty
  @JsonView(View.All.class)
  public String alg;

  /**
   * If the index should be applied to the history too.
   */
  @JsonProperty
  @JsonView(View.All.class)
  public boolean indexHistory = false;

  /**
   * All properties that should be included in this index.
   */
  @JsonProperty
  @JsonView(View.All.class)
  public List<@NotNull IndexProperty> properties;

}