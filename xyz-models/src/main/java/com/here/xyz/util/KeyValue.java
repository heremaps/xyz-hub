/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.util;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public record KeyValue<K, V>(K key, V value) {
  public KeyValue<K, V> putToMap(Map<K, V> map) {
    map.put(key, value);
    return this;
  }

  public Map<K, V> extendMap(Map<K, V> map) {
    Map<K, V> extended = new HashMap<>(map);
    putToMap(extended);
    return extended;
  }

  public Map<K, V> toMap() {
    return toMap(this);
  }

  public static <K, V> Map<K, V> toMap(KeyValue<K, V>... entries) {
    Map<K, V> map = new HashMap<>();
    for (KeyValue<K, V> entry : entries)
      map.put(entry.key, entry.value);
    return map;
  }

  public Map<K, V> toUnmodifiableMap() {
    return toUnmodifiableMap(this);
  }

  public static <K, V> Map<K, V> toUnmodifiableMap(KeyValue<K, V>... entries) {
    return Collections.unmodifiableMap(toMap(entries));
  }
}
