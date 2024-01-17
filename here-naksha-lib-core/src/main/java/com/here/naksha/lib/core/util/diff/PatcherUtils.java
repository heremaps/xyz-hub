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
package com.here.naksha.lib.core.util.diff;

import java.util.Iterator;
import java.util.Map.Entry;

public class PatcherUtils {
  public static Difference removeAllRemoveOp(Difference difference) {
    if (difference instanceof RemoveOp) {
      return null;
    } else if (difference instanceof ListDiff) {
      final ListDiff listdiff = (ListDiff) difference;
      final Iterator<Difference> iterator = listdiff.iterator();
      while (iterator.hasNext()) {
        Difference next = iterator.next();
        if (next == null) continue;
        next = removeAllRemoveOp(next);
        if (next == null) iterator.remove();
      }
      return listdiff;
    } else if (difference instanceof MapDiff) {
      final MapDiff mapdiff = (MapDiff) difference;
      final Iterator<Entry<Object, Difference>> iterator =
          mapdiff.entrySet().iterator();
      while (iterator.hasNext()) {
        Entry<Object, Difference> next = iterator.next();
        next.setValue(removeAllRemoveOp(next.getValue()));
        if (next.getValue() == null) iterator.remove();
      }
      return mapdiff;
    }
    return difference;
  }
}
