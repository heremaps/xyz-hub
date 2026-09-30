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

package com.here.xyz.hub.rest;

import static com.here.xyz.hub.config.TagConfigClient.versionTagId;
import static com.here.xyz.models.hub.Tag.isValidId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.here.xyz.models.hub.Ref;
import org.junit.Test;

public class TagTest {

  @Test
  public void testInvalidTagNames() {
    assertFalse(isValidId(""));
    assertFalse(isValidId("  "));
    assertFalse(isValidId("  abc"));
    assertFalse(isValidId("  abc"));
    assertFalse(isValidId("1abc"));
    assertFalse(isValidId("abc "));
    assertFalse(isValidId("abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijX"));
    assertFalse(isValidId("HEAD"));
    assertFalse(isValidId("*"));
    assertFalse(isValidId("some:tag"));
  }

  @Test
  public void testValidTagNames() {
    assertTrue(isValidId("abcdefghij"));
    assertTrue(isValidId("head"));
    assertTrue(isValidId("a1bc"));
    assertTrue(isValidId("abc"));
    assertTrue(isValidId("a"));
    assertTrue(isValidId("#27"));
    assertTrue(isValidId("_abc"));
  }

  @Test
  public void testValidVersionTagNames() {
    assertTrue(isValidId(versionTagId(new Ref(42), "extension hrn:here:data::olp-here:catalog:layer")));
    assertTrue(isValidId(versionTagId(new Ref("b1:7"), "branch b2")));
    assertTrue(isValidId(versionTagId(new Ref(Long.MAX_VALUE), "extension " + "x".repeat(200))));
  }

  @Test
  public void testVersionTagNamesPerTaggedByAndVersion() {
    assertEquals(versionTagId(new Ref(42), "extension a"), versionTagId(new Ref(42), "extension a"));
    assertTrue(versionTagId(new Ref(42), "extension a").startsWith("main_42_"));
    assertTrue(versionTagId(new Ref("b1:7"), "branch b2").startsWith("b1_7_"));
    assertNotEquals(versionTagId(new Ref(42), "extension a"), versionTagId(new Ref(43), "extension a"));
    assertNotEquals(versionTagId(new Ref(42), "extension a"), versionTagId(new Ref(42), "extension b"));
    assertNotEquals(versionTagId(new Ref(42), "extension a"), versionTagId(new Ref(42), "branch a"));
  }
}
