/*
 * Copyright (C) 2017-2025 HERE Europe B.V.
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

package com.here.xyz.models.hub;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.here.xyz.models.hub.Ref.InvalidRef;
import com.here.xyz.XyzSerializable;
import org.junit.Test;

public class RefTests {
  @Test
  public void defaultReferenceIsHead() {
    for (String value : new String[]{null, "", "HEAD"}) {
      Ref ref = new Ref(value);
      assertTrue(ref.isSingleVersion());
      assertTrue(ref.isHead());
      assertFalse(ref.isAllVersions());
      assertEquals("HEAD", ref.toString());
      assertThrows(InvalidRef.class, ref::getVersion);
    }
  }

  @Test
  public void versionNumbersRoundTrip() throws Exception {
    for (long version : new long[]{0, 42, Long.MAX_VALUE}) {
      Ref ref = new Ref(Long.toString(version));
      assertEquals(version, ref.getVersion());
      assertEquals(new Ref(version), ref);
      assertFalse(ref.isHead());
      assertEquals("\"" + version + "\"", ref.serialize());
      assertEquals(ref, XyzSerializable.deserialize(ref.serialize(), Ref.class));
    }
    assertThrows(InvalidRef.class, () -> new Ref(-1));
  }

  @Test
  public void tagReferencesRemainSupported() {
    for (String tag : new String[]{"myTag", "a", "-1", "tag-with.dots"}) {
      Ref ref = new Ref(tag);
      assertTrue(ref.isTag());
      assertEquals(tag, ref.getTag());
      assertEquals(tag, ref.toString());
      assertThrows(InvalidRef.class, ref::getVersion);
    }
  }

  @Test
  public void rangesAndAllVersionsRemainSupported() throws Exception {
    for (String value : new String[]{"0..42", "42..42", "42..HEAD", "*"}) {
      Ref ref = new Ref(value);
      assertEquals(value, ref.toString());
      assertFalse(ref.isSingleVersion());
      assertEquals(ref, XyzSerializable.deserialize(ref.serialize(), Ref.class));
    }
    assertEquals(new Ref(0, 42), new Ref("0..42"));
    assertTrue(new Ref("*").isAllVersions());
  }

  @Test
  public void legacySpaceReferencesAreNormalized() {
    for (String value : new String[]{"0", "42", "HEAD", "*", "0..HEAD", "5..10"})
      assertEquals(new Ref(value), new Ref("main:" + value));
  }

  @Test
  public void tagRangeEndpointsRemainSupported() throws Exception {
    for (String value : new String[]{"1..tagV2", "tagV1..42", "tagV1..tagV2", "tagV2..tagV2", "tagV1..HEAD"}) {
      Ref ref = new Ref(value);
      assertTrue(ref.isRange());
      assertEquals(value, ref.toString());
      assertEquals(ref, XyzSerializable.deserialize(ref.serialize(), Ref.class));
    }
    assertEquals(new Ref("1..tagV2"), new Ref(new Ref(1), new Ref("tagV2")));
    assertEquals(new Ref("tagV1..HEAD"), new Ref(new Ref("tagV1"), new Ref("HEAD")));
    assertThrows(InvalidRef.class, () -> new Ref(new Ref("*"), new Ref(42)));
    assertThrows(InvalidRef.class, () -> new Ref(new Ref(0), new Ref("*")));
  }

  @Test
  public void namedBranchReferencesAreRejected() {
    for (String value : new String[]{"branch:42", "branch:HEAD", "branch:*", "branch:0..HEAD", "~1:42", "!branch:HEAD"})
      assertThrows(value, InvalidRef.class, () -> new Ref(value));
  }

  @Test
  public void invalidReferencesAndRangesAreRejected() {
    for (String value : new String[]{"main:tag", "main:42:1", "42:1", "5abc", "main", "0..", "..1", "0..1..2",
        "HEAD..42", "42..0", "0..*"})
      assertThrows(value, InvalidRef.class, () -> new Ref(value));
  }

  @Test
  public void tagsNoLongerPersistBranchIdentifiers() {
    Tag tag = new Tag().withId("myTag").withVersionRef(new Ref(42));
    assertEquals(42, tag.getVersion());
    assertFalse(tag.toMap(XyzSerializable.Static.class).containsKey("branchId"));
  }
}
