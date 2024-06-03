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
package com.here.naksha.lib.core.storage;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.jetbrains.annotations.NotNull;

/**
 * A closable iterator.
 * @param <E> The element-type to iterate.
 */
@Deprecated
public interface ClosableIterator<E> extends Iterator<E>, Closeable {

  /**
   * Close the iterator and the underlying resource.
   */
  @Override
  void close();

  /**
   * Helper method to read all elements from the iterator.
   *
   * @return The read elements, maybe empty.
   */
  default @NotNull List<E> readAll() {
    return read(0, Integer.MAX_VALUE);
  }

  /**
   * Helper method to read all elements from the iterator.
   *
   * @param skip The amount of elements to skip before reading.
   * @return The read elements, maybe empty.
   */
  default @NotNull List<E> readAll(int skip) {
    return read(skip, Integer.MAX_VALUE);
  }

  /**
   * Helper method to read all elements from the iterator.
   *
   * @param limit The maximum amount of elements to read.
   * @return The read elements, maybe empty.
   */
  default @NotNull List<E> read(int limit) {
    return read(0, limit);
  }

  /**
   * Helper method to read all elements from the iterator.
   *
   * @param skip The amount of elements to skip before reading.
   * @param limit The maximum amount of elements to read.
   * @return The read elements, maybe empty.
   */
  default @NotNull List<E> read(int skip, int limit) {
    final ArrayList<E> list = new ArrayList<>();
    while (hasNext() && skip > 0) {
      next();
    }
    while (hasNext() && list.size() < limit) {
      list.add(next());
    }
    return list;
  }
}
