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
package com.here.naksha.lib.core.util.json;

import static com.here.naksha.lib.core.util.FibMap.EMPTY;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nonnull;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/** An common implementation of a key-set. */
public class MapKeySet<K, V> implements Set<@NotNull K> {

  MapKeySet(@NotNull Map<K, V> map) {
    this.map = map;
    this.mapSet = map.entrySet();
  }

  @JsonIgnore
  protected final @NotNull Map<K, V> map;

  @JsonIgnore
  protected final @NotNull Set<Map.Entry<@NotNull K, @Nullable V>> mapSet;

  @JsonIgnore
  protected @Nullable MapEntry<K, V> entry;

  private @NotNull MapEntry<K, V> entry(@NotNull K key) {
    MapEntry<K, V> entry = this.entry;
    if (entry == null) {
      return this.entry = new MapEntry<>(map, key, null);
    }
    entry.key = key;
    entry.value = null;
    return entry;
  }

  @Override
  public int size() {
    return mapSet.size();
  }

  @Override
  public boolean isEmpty() {
    return mapSet.isEmpty();
  }

  @Override
  public boolean contains(Object o) {
    //noinspection SuspiciousMethodCalls
    return map.containsKey(o);
  }

  @Nonnull
  @Override
  public Iterator<@NotNull K> iterator() {
    final Iterator<Entry<@NotNull K, @Nullable V>> it = mapSet.iterator();
    return new Iterator<>() {
      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public K next() {
        return it.next().getKey();
      }
    };
  }

  @Override
  public @Nullable Object @NotNull [] toArray() {
    return toArray(EMPTY);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> @NotNull T @NotNull [] toArray(final @NotNull T @NotNull [] original) {
    return (T[]) JsonUtils.mapToArray(map, original, JsonUtils::extractKey);
  }

  @Override
  public boolean add(@NotNull K s) {
    return mapSet.add(entry(s));
  }

  @Override
  public boolean remove(Object o) {
    //noinspection SuspiciousMethodCalls
    if (map.containsKey(o)) {
      map.remove(o);
      return true;
    }
    return false;
  }

  @Override
  public boolean containsAll(@NotNull Collection<?> c) {
    for (final Object key : c) {
      //noinspection SuspiciousMethodCalls
      if (!map.containsKey(key)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean addAll(@NotNull Collection<? extends @NotNull K> c) {
    boolean modified = false;
    for (final K key : c) {
      if (map.putIfAbsent(key, null) == null) {
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public boolean retainAll(@NotNull Collection<?> c) {
    throw new UnsupportedOperationException("retainAll");
  }

  @Override
  public boolean removeAll(@NotNull Collection<?> c) {
    boolean modified = false;
    for (final Object key : c) {
      //noinspection SuspiciousMethodCalls
      if (map.containsKey(key)) {
        //noinspection SuspiciousMethodCalls
        map.remove(key);
        modified = true;
      }
    }
    return modified;
  }

  @Override
  public void clear() {
    map.clear();
  }
}
