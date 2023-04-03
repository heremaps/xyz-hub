package com.here.xyz.events;

import org.jetbrains.annotations.NotNull;

public enum QueryParameterType {
  ANY(0),
  ANY_OR_NULL(1),

  STRING(2),
  STRING_OR_NULL(3),

  BOOLEAN(4),
  BOOLEAN_OR_NULL(5),

  LONG(6),
  LONG_OR_NULL(7),

  DOUBLE(8),
  DOUBLE_OR_NULL(9);

  QueryParameterType(int i) {
    this.nullable = (i & 1) == 1;
    this.maybeString = i < 2 || i == 2 || i == 3;
    this.maybeBoolean = i < 2 || i == 4 || i == 5;
    this.maybeNumber = i < 2 || i == 6 || i == 7 || i == 8 || i == 9;
    this.maybeLong = i < 2 || i == 6 || i == 7;
    this.maybeDouble = i < 2 || i == 8 || i == 9;
    if (i == 2 || i == 3) {
      string = "string";
    } else if (i == 4 || i == 5) {
      string = "boolean";
    } else if (i == 6 || i == 7) {
      string = "long";
    } else if (i == 8 || i == 9) {
      string = "double";
    } else {
      string = "any";
    }
  }

  public final boolean nullable;
  public final boolean maybeString;
  public final boolean maybeBoolean;
  public final boolean maybeNumber;
  public final boolean maybeLong;
  public final boolean maybeDouble;
  private final @NotNull String string;

  @Override
  public @NotNull String toString() {
    return string;
  }
}
