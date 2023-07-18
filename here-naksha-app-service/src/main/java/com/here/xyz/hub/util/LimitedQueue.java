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
package com.here.xyz.hub.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.LongAdder;

/**
 * A queue with limits on the number of elements and the their size.
 */
public class LimitedQueue<E extends ByteSizeAware> implements ByteSizeAware {

  public LimitedQueue(long maxSize, long maxByteSize) {
    this.maxSize = maxSize;
    this.maxByteSize = maxByteSize;
  }

  private final ConcurrentLinkedQueue<E> _queue = new ConcurrentLinkedQueue<>();
  private final LongAdder byteSize = new LongAdder();
  private long maxByteSize;
  private long maxSize;

  /**
   * Adds an element and optionally returns the elements, which had to be discarded to accommodate the new one.
   *
   * @return The elements, which had to be discarded to accommodate the new one.
   */
  public List<E> add(E element) {
    // If the maximum queue size is not large enough to fit the element, then the new element needs to be discarded.
    if (element.getByteSize() > maxByteSize) {
      return Collections.singletonList(element);
    }

    // Add the element and update the size
    byteSize.add(element.getByteSize());
    // Note: When a context switch happens exactly at this point, then we have a disconnection between the
    //       added element and the byte size!
    _queue.add(element);

    return discard();
  }

  /**
   * Removes the head of the queue and returns it.
   *
   * @return The head of the queue or null if the queue is empty
   */
  public E remove() {
    E removed = _queue.poll();
    if (removed != null) {
      byteSize.add(-removed.getByteSize());
    }
    return removed;
  }

  private List<E> discard() {
    List<E> discardedElements = new ArrayList<>();

    // Check if older elements need to be discarded to make space for the new one.
    while (byteSize.longValue() > maxByteSize || _queue.size() > maxSize) {
      E discarded = remove();
      if (discarded != null) {
        discardedElements.add(discarded);
      }
    }

    return discardedElements;
  }

  public List<E> setMaxByteSize(long byteSize) {
    if (byteSize < 0) {
      throw new IllegalArgumentException("The maximum byte size of a queue can not be negative.");
    }
    maxByteSize = byteSize;
    return discard();
  }

  public long getMaxByteSize() {
    return maxByteSize;
  }

  public List<E> setMaxSize(long size) {
    if (size < 0) {
      throw new IllegalArgumentException("The maximum size of a queue can not be negative.");
    }
    maxSize = size;
    return discard();
  }

  public long getMaxSize() {
    return maxSize;
  }

  public long getSize() {
    return _queue.size();
  }

  /**
   * Returns the estimated size of the queue in byte. Be aware that the value can be wrong, dependent on context switched happening
   *
   * @return
   */
  @Override
  public long getByteSize() {
    return byteSize.longValue();
  }
}
