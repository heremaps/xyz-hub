package com.here.xyz.events;

import org.jetbrains.annotations.NotNull;

public enum PropertyQueryOp {
  EQUALS("eq"),
  NOT_EQUALS("ne"),
  GREATER_THAN("gt"),
  GREATER_THAN_EQUALS("gte"),
  LESS_THAN("lt"),
  LESS_THAN_EQUALS("lte"),
  CONTAINS("cs"),
  IN("in");

  PropertyQueryOp(@NotNull String text) {
    this.text = text;
  }

  /**
   * The text representation of the query operation.
   */
  public final @NotNull String text;
}