/*
 * Copyright (C) 2017-2026 HERE Europe B.V.
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

import com.here.xyz.models.hub.DataReference;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;

import static com.here.xyz.hub.util.DataReferenceValidator.validateDataReference;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.argumentSet;

class DataReferenceValidatorTest {

  private static final UUID correctReferenceId = UUID.fromString("38dff813-722a-41c0-afc0-7dfcc6a7b092");

  private static DataReference correctNonPatchReference() {
    return new DataReference()
      .withId(correctReferenceId)
      .withEntityId("entityId-A")
      .withPatch(false)
      .withEndVersion(5)
      .withObjectType("object-type-A")
      .withContentType("content-type-A")
      .withLocation("location-A")
      .withSourceSystem("source-system-A")
      .withTargetSystem("target-system-A");
  }

  private static DataReference correctPatchReference() {
    return new DataReference()
      .withId(correctReferenceId)
      .withEntityId("entityId-A")
      .withPatch(true)
      .withStartVersion(1)
      .withEndVersion(5)
      .withObjectType("object-type-A")
      .withContentType("content-type-A")
      .withLocation("location-A")
      .withSourceSystem("source-system-A")
      .withTargetSystem("target-system-A");
  }

  static Stream<Arguments> correctObjects() {
    return Stream.of(
      argumentSet("correct patch reference", correctPatchReference()),
      argumentSet("correct non-patch reference", correctNonPatchReference())
    );
  }

  @ParameterizedTest
  @MethodSource("correctObjects")
  void shouldNotRaiseViolationsForCorrectObject(DataReference dataReference) {
    // when
    Collection<String> violations = validateDataReference(dataReference);

    // then
    assertThat(violations).isEmpty();
  }

  static Stream<Arguments> incorrectObjects() {
    return Stream.of(
      argumentSet(
        "missing endVersion",
        correctPatchReference().withEndVersion(null),
        Set.of("Data Reference must contain an endVersion")
      ),
      argumentSet(
        "negative endVersion",
        correctNonPatchReference().withEndVersion(-1),
        Set.of("Data Reference endVersion must be a non-negative integer")
      ),
      argumentSet(
        "negative startVersion",
        correctPatchReference().withStartVersion(-1),
        Set.of("Data Reference startVersion must be a non-negative integer")
      ),
      argumentSet(
        "startVersion higher than endVersion",
        correctPatchReference().withStartVersion(2).withEndVersion(1),
        Set.of("Data Reference startVersion must be an integer less than endVersion")
      ),
      argumentSet(
        "startVersion equal to endVersion",
        correctPatchReference().withStartVersion(1).withEndVersion(1),
        Set.of("Data Reference startVersion must be an integer less than endVersion")
      ),
      argumentSet(
        "startVersion present but reference is not a patch",
        correctNonPatchReference().withStartVersion(1),
        Set.of("Data Reference startVersion must only be set when isPatch is set to true")
      ),
      argumentSet(
        "startVersion missing but reference is a patch",
        correctPatchReference().withStartVersion(null),
        Set.of("Data Reference startVersion must be set when isPatch is set to true")
      ),
      argumentSet(
        "entityId missing in a patch reference",
        correctPatchReference().withEntityId(null),
        Set.of("Data Reference entityId must not be blank, empty or null")
      ),
      argumentSet(
        "entityId missing in a non-patch reference",
        correctNonPatchReference().withEntityId(null),
        Set.of("Data Reference entityId must not be blank, empty or null")
      ),
      argumentSet(
        "objectType missing in a patch reference",
        correctPatchReference().withObjectType(null),
        Set.of("Data Reference objectType must not be blank, empty or null")
      ),
      argumentSet(
        "objectType missing in a non-patch reference",
        correctNonPatchReference().withObjectType(null),
        Set.of("Data Reference objectType must not be blank, empty or null")
      ),
      argumentSet(
        "contentType missing in a patch reference",
        correctPatchReference().withContentType(null),
        Set.of("Data Reference contentType must not be blank, empty or null")
      ),
      argumentSet(
        "contentType missing in a non-patch reference",
        correctNonPatchReference().withContentType(null),
        Set.of("Data Reference contentType must not be blank, empty or null")
      ),
      argumentSet(
        "location missing in a patch reference",
        correctPatchReference().withLocation(null),
        Set.of("Data Reference location must not be blank, empty or null")
      ),
      argumentSet(
        "location missing in a non-patch reference",
        correctNonPatchReference().withLocation(null),
        Set.of("Data Reference location must not be blank, empty or null")
      ),
      argumentSet(
        "sourceSystem missing in a patch reference",
        correctPatchReference().withSourceSystem(null),
        Set.of("Data Reference sourceSystem must not be blank, empty or null")
      ),
      argumentSet(
        "sourceSystem missing in a non-patch reference",
        correctNonPatchReference().withSourceSystem(null),
        Set.of("Data Reference sourceSystem must not be blank, empty or null")
      ),
      argumentSet(
        "targetSystem missing in a patch reference",
        correctPatchReference().withTargetSystem(null),
        Set.of("Data Reference targetSystem must not be blank, empty or null")
      ),
      argumentSet(
        "targetSystem missing in a non-patch reference",
        correctNonPatchReference().withTargetSystem(null),
        Set.of("Data Reference targetSystem must not be blank, empty or null")
      ),
      argumentSet(
        "missing fields in a non-patch reference",
        new DataReference().withPatch(false),
        Set.of(
          "Data Reference must contain an endVersion",
          "Data Reference entityId must not be blank, empty or null",
          "Data Reference objectType must not be blank, empty or null",
          "Data Reference contentType must not be blank, empty or null",
          "Data Reference location must not be blank, empty or null",
          "Data Reference sourceSystem must not be blank, empty or null",
          "Data Reference targetSystem must not be blank, empty or null"
        )
      ),
      argumentSet(
        "missing fields in a patch reference",
        new DataReference().withPatch(true),
        Set.of(
          "Data Reference must contain an endVersion",
          "Data Reference startVersion must be set when isPatch is set to true",
          "Data Reference entityId must not be blank, empty or null",
          "Data Reference objectType must not be blank, empty or null",
          "Data Reference contentType must not be blank, empty or null",
          "Data Reference location must not be blank, empty or null",
          "Data Reference sourceSystem must not be blank, empty or null",
          "Data Reference targetSystem must not be blank, empty or null"
        )
      )
    );
  }

  @ParameterizedTest
  @MethodSource("incorrectObjects")
  void shouldRaiseViolationsForIncorrectObject(DataReference dataReference, Set<String> expectedViolations) {
    // when
    Collection<String> violations = validateDataReference(dataReference);

    // then
    assertThat(violations).containsExactlyInAnyOrderElementsOf(expectedViolations);
  }

}
