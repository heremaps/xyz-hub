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

import static com.here.naksha.lib.core.NakshaVersion.v2_0_6;

import com.here.naksha.lib.core.lambdas.F1;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * Some utility functions.
 */
public final class NakshaHelper {

  private static Boolean usesZGC;

  /**
   * Tests whether ZGC is used as garbage collector.
   *
   * @return {@code true} if ZGC is used as garbage collector; {@code false} otherwise.
   */
  public static boolean isUsingZgc() {
    if (usesZGC != null) {
      return usesZGC;
    }
    final List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
    for (final GarbageCollectorMXBean gcMxBean : gcMxBeans) {
      if (gcMxBean.getName().startsWith("ZGC")) {
        return usesZGC = Boolean.TRUE;
      }
    }
    return usesZGC = Boolean.FALSE;
  }

  /**
   * Convert the given list into a map, ignore duplicates.
   *
   * @param list  The list to convert.
   * @param <KEY> The value-type of the list to become the key-type of the map.
   * @return The map with the key being the value from the list and the value being {@link Boolean#TRUE}.
   */
  @AvailableSince(v2_0_6)
  public static <KEY> Map<KEY, Boolean> listToMap(@NotNull List<KEY> list) {
    final LinkedHashMap<KEY, Boolean> map = new LinkedHashMap<>();
    for (final KEY k : list) {
      map.put(k, Boolean.TRUE);
    }
    return map;
  }

  /**
   * Convert the given list into a map, duplicates override previously added values.
   *
   * @param list  The list to convert.
   * @param unpack The method to unpack the key from the elements.
   * @param <KEY> The key-type.
   * @param <E> The element-type found in the list.
   * @return The map with the key being extracted from the element, the value is the element.
   */
  @AvailableSince(v2_0_6)
  public static <KEY, E> Map<KEY, E> listToMap(@NotNull List<E> list, @NotNull F1<KEY, E> unpack) {
    final LinkedHashMap<KEY, E> map = new LinkedHashMap<>();
    for (final E e : list) {
      map.put(unpack.call(e), e);
    }
    return map;
  }
}
