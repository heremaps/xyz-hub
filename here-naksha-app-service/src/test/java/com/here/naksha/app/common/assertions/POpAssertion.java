package com.here.naksha.app.common.assertions;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.here.naksha.lib.core.models.storage.OpType;
import com.here.naksha.lib.core.models.storage.POp;
import com.here.naksha.lib.core.models.storage.POpType;
import java.util.List;
import java.util.function.Consumer;

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
