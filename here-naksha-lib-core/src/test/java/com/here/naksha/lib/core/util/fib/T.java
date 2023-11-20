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

import static com.here.naksha.lib.core.util.fib.FibSet.STRONG;
import static com.here.naksha.lib.core.util.fib.FibSetOp.GET;
import static com.here.naksha.lib.core.util.fib.FibSetOp.PUT;

import java.util.*;

public class T {
  public static void main(String[] args) {
    // given
    // cache (262144)
    final FibSet<String, FibMapEntry<String, String>> fibSet = new FibSet<>(FibMapEntry::new);
    System.out.println(Runtime.getRuntime().maxMemory());
    System.out.println(Runtime.getRuntime().totalMemory());
    //    long size = 10L;
    long size = 40_621_440L;

    //    fibSet.execute(PUT, "foo0", STRONG);
    //    fibSet.execute(PUT, "foo10", STRONG);
    //        assertNotNull(fibSet.execute(GET, "foo0", STRONG));
    //    assertNotNull(fibSet.execute(GET, "foo10", STRONG));

    long hit = 0;
    long miss = 0;

    for (int i = 0; i < size; i++) {
      fibSet.execute(PUT, "foo" + i, STRONG);
      //      put("foo" + i);
    }

    for (int i = 0; i < size; i++) {
      FibMapEntry fibEntry = fibSet.execute(GET, "foo" + i, STRONG);
      //      String fibEntry = get("foo" + i);
      hit += 1;
      if (fibEntry == null) {
        miss += 1;
      }
    }
    System.out.printf(
        "Hit: %s; Miss: %s; Misses ratio: %s%%;%n", hit, miss, Math.round(miss * 10000d / hit) / 100d);

    Map<Integer, Stats> stats = new LinkedHashMap<Integer, Stats>();
    stats(fibSet.root, 0, stats);

    System.out.println(String.format(
        "  Lvl   |          entries  |         lpt     |        total    |   entries/total ratio "));
    stats.entrySet().stream()
        .sorted(Map.Entry.comparingByKey())
        .forEach(entry -> System.out.printf("    %s   |  %s %n", entry.getKey(), entry.getValue()));
  }

  private static void stats(Object[] arr, int lvl, Map<Integer, Stats> statistics) {
    long levelEntries = 0;
    long lptCount = 0;
    for (int i = 0; i < arr.length; i++) {

      if (arr[i] instanceof Object[] subarr) {
        stats(subarr, lvl + 1, statistics);
      } else if (arr[i] instanceof final FibLinearProbeTable lpt) {
        lptCount += Arrays.stream(lpt.entries).filter(Objects::nonNull).count();
      } else if (arr[i] instanceof final FibEntry entry) {
        levelEntries += 1;
      }
    }
    if (!statistics.containsKey(lvl)) {
      statistics.put(lvl, new Stats());
    }
    Stats s = statistics.get(lvl);
    s.entries += levelEntries;
    s.total += 16;
    s.lptCount += lptCount;
  }

  Map<Integer, Stats> stats = new LinkedHashMap<Integer, Stats>();

  private static class Stats {
    Long total = 0l;
    Long entries = 0l;
    Long lptCount = 0l;

    @Override
    public String toString() {
      return String.format(
          "%1$15s  | %2$15s | %3$15s | %4$15s%%",
          entries, lptCount, total, Math.round(entries * 10000d / total) / 100d);
    }
  }

  Map<String, String> cache = new HashMap<>();
}
