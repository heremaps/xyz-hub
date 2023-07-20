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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.jetbrains.annotations.ApiStatus.AvailableSince;
import org.jetbrains.annotations.NotNull;

/**
 * Some utility functions.
 */
public final class NakshaHelper {

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
}
