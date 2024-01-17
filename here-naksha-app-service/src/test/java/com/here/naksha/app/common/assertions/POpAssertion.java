package com.here.naksha.app.common.assertions;

import com.here.naksha.lib.core.models.storage.*;

import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;

public record POpAssertion(POp subject) {

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
    assertTrue( indexed ? !(pref instanceof NonIndexedPRef) : pref instanceof NonIndexedPRef);
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
