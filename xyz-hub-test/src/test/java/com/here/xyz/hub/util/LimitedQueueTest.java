/*
 * Copyright (C) 2017-2019 HERE Europe B.V.
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.List;
import org.junit.Test;

public class LimitedQueueTest {

  public class TestElement implements ByteSizeAware {

    private final long byteSize;

    TestElement(long byteSize) {
      this.byteSize = byteSize;
    }

    @Override
    public long getByteSize() {
      return byteSize;
    }
  }

  @Test
  public void addTooLargeElement() {
    LimitedQueue<TestElement> queue = new LimitedQueue<>(3, 100);
    TestElement tooLargeElement = new TestElement(101);
    List<TestElement> discarded = queue.add( tooLargeElement );
    assertEquals("Only the inserted element must be discarded.", 1, discarded.size());
    assertEquals( "The inserted element must be discarded.",  discarded.get(0), tooLargeElement);
  }

  @Test
  public void addTooManyElements() {
    LimitedQueue<TestElement> queue = new LimitedQueue<>(3, 100);
    TestElement element1 = new TestElement(1);
    TestElement element2 = new TestElement(1);
    TestElement element3 = new TestElement(1);

    List<TestElement> discarded;
    discarded = queue.add(element1);
    assertEquals("Expected was that no elements are discarded.", 0, discarded.size());
    assertEquals("Expected was 1 element.", 1, queue.getSize());
    discarded = queue.add(element2);
    assertEquals("Expected was that no elements are discarded.", 0, discarded.size());
    assertEquals("Expected were 2 elements.", 2, queue.getSize());
    discarded = queue.add(element3);
    assertEquals("Expected was that no elements are discarded.", 0, discarded.size());
    assertEquals("Expected were 3 elements.", 3, queue.getSize());

    discarded = queue.add(new TestElement(1));
    assertEquals("Expected were 3 elements.", 3, queue.getSize());
    assertEquals(1, discarded.size());
    assertEquals("The first element must be discarded.", discarded.get(0), element1);
    discarded = queue.add(new TestElement(1));
    assertEquals("Expected were 3 elements.", 3, queue.getSize());
    assertEquals("The first element must be discarded.", discarded.get(0), element2);
  }

  @Test
  public void addTooManyBytes() {
    LimitedQueue<TestElement> queue = new LimitedQueue<>(100, 100);
    TestElement element1 = new TestElement(40);
    TestElement element2 = new TestElement(40);
    TestElement element3 = new TestElement(40);

    List<TestElement> discarded;
    discarded = queue.add(element1);
    assertEquals("Expected was that no elements are discarded.", 0, discarded.size());
    assertEquals("Expected was 1 element.", 1, queue.getSize());
    discarded = queue.add(element2);
    assertEquals("Expected was that no elements are discarded.", 0, discarded.size());
    assertEquals("Expected were 2 elements.", 2, queue.getSize());
    discarded = queue.add(element3);
    assertEquals("Expected was that no elements are discarded.", 1, discarded.size());
    assertEquals("Expected were 2 elements.", 2, queue.getSize());
    assertEquals("The first element must be discarded.", discarded.get(0), element1);
  }

  @Test
  public void remove() {
    LimitedQueue<TestElement> queue = new LimitedQueue<>(3, 100);
    TestElement element1 = new TestElement(1);
    TestElement element2 = new TestElement(1);
    TestElement element3 = new TestElement(1);

    List<TestElement> discarded;
    discarded = queue.add(element1);
    assertEquals("Expected was that no elements are discarded.", 0, discarded.size());
    assertEquals("Expected was 1 element.", 1, queue.getSize());
    discarded = queue.add(element2);
    assertEquals("Expected was that no elements are discarded.", 0, discarded.size());
    assertEquals("Expected were 2 elements.", 2, queue.getSize());
    discarded = queue.add(element3);
    assertEquals("Expected was that no elements are discarded.", 0, discarded.size());
    assertEquals("Expected were 3 elements.", 3, queue.getSize());

    TestElement removed = queue.remove();
    assertEquals("Expected element 1 was removed", removed, element1);
    assertEquals("Expected were 2 elements.", 2, queue.getSize());
    assertEquals("Expected were 2 bytes.", 2, queue.getByteSize());

    removed = queue.remove();
    assertEquals("Expected element 2 was removed", removed, element2);
    assertEquals("Expected were 1 elements.", 1, queue.getSize());
    assertEquals("Expected were 1 bytes.", 1, queue.getByteSize());

    removed = queue.remove();
    assertEquals("Expected element 3 was removed", removed, element3);
    assertEquals("Expected were 0 elements.", 0, queue.getSize());
    assertEquals("Expected were 0 bytes.", 0, queue.getByteSize());

    removed = queue.remove();
    assertNull("Expected no elements were removed", removed);
    assertEquals("Expected were 0 elements.", 0, queue.getSize());
    assertEquals("Expected were 0 bytes.", 0, queue.getByteSize());
  }

  @Test
  public void setMaxByteSize() {
    LimitedQueue<TestElement> queue = new LimitedQueue<>(3, 100);
    TestElement element1 = new TestElement(1);
    TestElement element2 = new TestElement(1);
    TestElement element3 = new TestElement(1);

    queue.add(element1);
    queue.add(element2);
    queue.add(element3);

    List<TestElement> discarded;
    discarded = queue.setMaxByteSize(1);
    assertEquals("Expected was that 2 elements are discarded.", 2, discarded.size());
    assertEquals("Expected was 1 element.", 1, queue.getSize());
    assertEquals("Expected was that element 1 was discarded first.", element1, discarded.get(0));
    assertEquals("Expected was that element 2 was discarded second.", element2, discarded.get(1));
  }

  @Test
  public void getSizes() {
    LimitedQueue<TestElement> queue = new LimitedQueue<>(50, 100);
    TestElement element1 = new TestElement(40);
    TestElement element2 = new TestElement(40);

    queue.add(element1);
    queue.add(element2);

    assertEquals("Expected was a size of 80 bytes",80, queue.getByteSize());
    assertEquals("Expected was a size of 100 bytes",100, queue.getMaxByteSize());
    assertEquals("Expected was a size of 50",50, queue.getMaxSize());
    assertEquals("Expected was a size of 2",2, queue.getSize());
  }

  @Test
  public void setMaxSize() {
    LimitedQueue<TestElement> queue = new LimitedQueue<>(3, 100);
    TestElement element1 = new TestElement(1);
    TestElement element2 = new TestElement(1);
    TestElement element3 = new TestElement(1);

    queue.add(element1);
    queue.add(element2);
    queue.add(element3);

    List<TestElement> discarded;
    discarded = queue.setMaxSize(1);
    assertEquals("Expected was that 2 elements are discarded.", 2, discarded.size());
    assertEquals("Expected was 1 element.", 1, queue.getSize());
    assertEquals("Expected was that element 1 was discarded first.", element1, discarded.get(0));
    assertEquals("Expected was that element 2 was discarded second.", element2, discarded.get(1));
  }
}
