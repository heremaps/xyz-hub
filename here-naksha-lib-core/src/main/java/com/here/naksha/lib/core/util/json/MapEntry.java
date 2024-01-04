/*
 * Copyright (C) 2017-2024 HERE Europe B.V.
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
package com.here.naksha.lib.core.util.json;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Map;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** Core implementation of a map-entry. */
public class MapEntry<K, V> implements Map.Entry<@NotNull K, @Nullable V> {

  public MapEntry(@NotNull Map<K, V> map, @NotNull K key, @Nullable V value) {
    this.map = map;
    this.key = key;
    this.value = value;
  }

  @JsonIgnore
  protected final @NotNull Map<K, V> map;

  @JsonIgnore
  protected @NotNull K key;

  @JsonIgnore
  protected @Nullable V value;

  @Override
  public @NotNull K getKey() {
    return key;
  }

  @Override
  public @Nullable V getValue() {
    return value;
  }

  @Override
  public @Nullable V setValue(@Nullable V value) {
    final V old = map.put(key, value);
    this.value = value;
    return old;
  }
}
