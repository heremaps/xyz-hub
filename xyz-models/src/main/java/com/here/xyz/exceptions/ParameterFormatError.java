package com.here.xyz.exceptions;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ParameterFormatError extends Exception {

  public ParameterFormatError(@NotNull String message) {
    super(message);
  }

  public ParameterFormatError(@NotNull String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}