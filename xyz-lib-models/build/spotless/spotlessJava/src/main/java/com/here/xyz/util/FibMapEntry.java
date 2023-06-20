package com.here.xyz.util;


import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** An map entry implementation. */
public final class FibMapEntry implements Map.Entry<Object, Object> {

  /**
   * Creates a new fibonacci map entry.
   *
   * @param key The key.
   * @param value The value.
   */
  public FibMapEntry(@NotNull Object key, @Nullable Object value) {
    this.key = key;
    this.value = value;
  }

  /** The key we refer to. */
  @NotNull Object key;

  /** The value we refer to. */
  @Nullable Object value;

  @Override
  public @NotNull Object getKey() {
    return key;
  }

  @Override
  public @Nullable Object getValue() {
    return value;
  }

  /**
   * Change the key and value and return this.
   *
   * @param key The next key.
   * @param value The new value.
   * @return this.
   */
  public @NotNull FibMapEntry with(@NotNull Object key, @Nullable Object value) {
    this.key = key;
    this.value = value;
    return this;
  }

  @Override
  public @Nullable Object setValue(@Nullable Object value) {
    throw new UnsupportedOperationException("setValue");
  }
}
