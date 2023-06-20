package com.here.xyz.exceptions;


import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class ParameterError extends Exception {

  public ParameterError(@NotNull String message) {
    super(message);
  }

  public ParameterError(@NotNull String message, @Nullable Throwable cause) {
    super(message, cause);
  }
}
