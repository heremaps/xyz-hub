package com.here.xyz.events;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

// @see <a href="https://tools.ietf.org/html/rfc3986#section-2.2"></a>
public enum QueryOperator {
  /**
   * {@code &key}.
   */
  NONE(""),
  /**
   * {@code &key:=}.
   */
  ASSIGN(":"),
  /**
   * {@code &key:exists&...}.
   */
  EXISTS("exists"),
  /**
   * {@code &key:not-exists&...}.
   */
  NOT_EXISTS("not-exists"),
  /**
   * {@code &key:eq=}.
   */
  EQUALS("eq"),
  /**
   * {@code &key:ne=}.
   */
  NOT_EQUALS("ne"),
  /**
   * {@code &key:lt=}.
   */
  LESS_THAN("lt"),
  /**
   * {@code &key:lte=}.
   */
  LESS_THAN_OR_EQUALS("lte"),
  /**
   * {@code &key:gt=}.
   */
  GREATER_THAN("gt"),
  /**
   * {@code &key:gte=}.
   */
  GREATER_THAN_OR_EQUALS("gte"),
  /**
   * {@code &key:cs=}.
   */
  CONTAINS("cs"),
  /**
   * {@code &key:in=}.
   */
  IN("in");

  QueryOperator(@NotNull String text) {
    this.text = text;
  }

  public static @Nullable QueryOperator get(@NotNull String text) {
    for (final QueryOperator op : QueryOperator.values()) {
      if (op.text.equals(text)) {
        return op;
      }
    }
    return null;
  }

  public static @NotNull QueryOperator get(@NotNull String text, @NotNull QueryOperator alt) {
    for (final QueryOperator op : QueryOperator.values()) {
      if (op.text.equals(text)) {
        return op;
      }
    }
    return alt;
  }

  /**
   * The full operation.
   */
  public final @NotNull String text;

  @Override
  public @NotNull String toString() {
    return text;
  }
}