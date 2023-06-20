package com.here.xyz.util;


import java.util.HashMap;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Params extends HashMap<@NotNull String, @Nullable Object> {

  public @NotNull Params with(@NotNull String key, @Nullable Object value) {
    put(key, value);
    return this;
  }
}
