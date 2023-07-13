/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 * License-Filename: LICENSE
 */
package com.here.naksha.lib.core.util.fib;

import com.here.naksha.lib.core.INaksha;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Map;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A thread safe entry for the {@link FibSet}, can be used as concurrent hash-map.
 */
@AvailableSince(INaksha.v2_0_5)
@SuppressWarnings("unchecked")
public class FibMapEntry<K, V> extends FibEntry<K> implements Map.Entry<K, V> {

  /**
   * Create a new empty map entry.
   *
   * @param key the key.
   */
  @AvailableSince(INaksha.v2_0_5)
  public FibMapEntry(@NotNull K key) {
    super(key);
    this.value = null;
  }

  /**
   * Create a new entry for a {@link FibSet}.
   *
   * @param key   the key.
   * @param value the value.
   */
  @AvailableSince(INaksha.v2_0_5)
  public FibMapEntry(@NotNull K key, @Nullable V value) {
    super(key);
    this.value = value;
  }

  /**
   * The current value.
   */
  @AvailableSince(INaksha.v2_0_5)
  @Nullable
  protected Object value;

  @Override
  public @NotNull K getKey() {
    return key;
  }

  @Override
  public @Nullable V getValue() {
    return (V) value;
  }

  @Override
  public @Nullable V setValue(@Nullable V value) {
    V old;
    do {
      old = (V) VALUE.get(this);
    } while (!VALUE.compareAndSet(this, old, value));
    return old;
  }

  /**
   * Tries an atomic update of the value of the key.
   *
   * @param expected the expected value.
   * @param value    the new value to set.
   * @return {@code true}, if the operation succeeded; false otherwise.
   */
  @AvailableSince(INaksha.v2_0_5)
  public boolean compareAndSet(@Nullable V expected, @Nullable V value) {
    return VALUE.compareAndSet(this, expected, value);
  }

  /**
   * The {@link VarHandle} to the value for atomic operations.
   */
  @AvailableSince(INaksha.v2_0_5)
  protected static final @NotNull VarHandle VALUE;

  static {
    try {
      VALUE = MethodHandles.lookup().findVarHandle(FibMapEntry.class, "value", Object.class);
    } catch (Exception e) {
      throw new InternalError(e);
    }
  }
}
