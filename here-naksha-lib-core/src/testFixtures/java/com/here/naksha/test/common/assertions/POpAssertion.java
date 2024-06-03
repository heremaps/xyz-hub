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
package com.here.naksha.test.common.assertions;

import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.lib.core.models.storage.*;
import java.util.List;
import java.util.function.Consumer;

public class POpAssertion {

  private final POp subject;

  public POpAssertion(POp subject) {
    this.subject = subject;
  }

  public POp getPOp() {
    return this.subject;
  }

  public static POpAssertion assertThatOperation(POp subject) {
    assertNotNull(subject);
    return new POpAssertion(subject);
  }

  public POpAssertion existsWithTagName(String tagName) {
    return hasType(POpType.EXISTS).hasTagName(tagName);
  }

  public POpAssertion hasType(OpType expectedOpType) {
    assertEquals(expectedOpType, subject.op());
    return this;
  }

  public POpAssertion hasTagName(String expectedTagName) {
    assertNotNull(subject.getPropertyRef());
    assertEquals(expectedTagName, subject.getPropertyRef().getTagName());
    return this;
  }

  public POpAssertion hasPRef(boolean indexed) {
    final PRef pref = subject.getPropertyRef();
    assertNotNull(pref);
    assertTrue(indexed ? !(pref instanceof NonIndexedPRef) : pref instanceof NonIndexedPRef);
    return this;
  }

  public POpAssertion hasPRef(PRef expected) {
    assertNotNull(expected);
    assertEquals(expected, subject.getPropertyRef());
    return this;
  }

  public POpAssertion hasPRefWithPath(String[] path) {
    hasPRef(true);
    assertArrayEquals(path, subject.getPropertyRef().getPath().toArray());
    return this;
  }

  public POpAssertion hasNonIndexedPRefWithPath(String[] path) {
    hasPRef(false);
    assertArrayEquals(path, subject.getPropertyRef().getPath().toArray());
    return this;
  }

  public POpAssertion hasValue(Number value) {
    assertEquals(value, subject.getValue());
    return this;
  }

  public POpAssertion hasValue(String value) {
    assertEquals(value, subject.getValue());
    return this;
  }

  @SafeVarargs
  public final POpAssertion hasChildrenThat(Consumer<POpAssertion>... childrenAssertions) {
    List<POp> subjects = subject.children();
    assertNotNull(subjects, "Expected multiple operations");
    assertEquals(subjects.size(), childrenAssertions.length, "Expecting single assertion per POp");
    for (int i = 0; i < subjects.size(); i++) {
      POpAssertion childAssertion = new POpAssertion(subjects.get(i));
      childrenAssertions[i].accept(childAssertion);
    }
    return this;
  }
}
