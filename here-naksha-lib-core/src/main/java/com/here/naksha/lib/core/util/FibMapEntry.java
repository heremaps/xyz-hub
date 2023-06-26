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
package com.here.naksha.lib.core.util;

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
  @NotNull
  Object key;

  /** The value we refer to. */
  @Nullable
  Object value;

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
