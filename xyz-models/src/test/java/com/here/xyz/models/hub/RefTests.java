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

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class RefTests {
  private static final String MAIN_BRANCH = "main";
  private static final String HEAD_VERSION = "HEAD";
  private static final String ALL_VERSIONS = "*";
  private static final String[] VALID_BRANCH_IDS = new String[]{"someBranchId", "a", "-1", "~1", "5someBranchId", MAIN_BRANCH, "", null};
  private static final String[] INVALID_BRANCH_IDS = new String[]{"42", "some:BranchId", ALL_VERSIONS, HEAD_VERSION};
  private static final String[] VALID_VERSIONS = new String[]{"42", ALL_VERSIONS, HEAD_VERSION, "", null};
  private static final String[] INVALID_VERSIONS = new String[]{"-42", "abc", "5abc", "abc5"};

  private static boolean canBeParsedAsLong(String maybeALong) {
    try {
      Long.parseLong(maybeALong);
      return true;
    }
    catch (NumberFormatException e) {
      return false;
    }
  }

  private static boolean canBeParsedAsNonNegativeLong(String maybeALong) {
    if (canBeParsedAsLong(maybeALong))
      return Long.parseLong(maybeALong) >= 0;
    return false;
  }

  private static boolean isValidVersion(String version) {
    return HEAD_VERSION.equals(version) || ALL_VERSIONS.equals(version) || canBeParsedAsNonNegativeLong(version);
  }

  private static List<String> crossProduct(String[] setA, String[] setB) {
    List<String> crossProduct = new ArrayList<>();
    for (String a : setA)
      for (String b : setB)
        crossProduct.add((!isNullOrEmpty(a) ? a : "") + (!isNullOrEmpty(a) && !isNullOrEmpty(b) ? ":" : "") + (!isNullOrEmpty(b) ? b : ""));
    return crossProduct;
  }

  @Test
  public void simpleBranchPositive() {
    for (String branchId : VALID_BRANCH_IDS) {
      Ref ref = Ref.fromBranchId(branchId);
      assertEquals(isNullOrEmpty(branchId) ? MAIN_BRANCH : branchId, ref.getBranch());
      assertTrue(ref.isSingleVersion());
      assertTrue(ref.isHead());
      assertFalse(ref.isAllVersions());
    }
  }

  @Test
  public void simpleVersionPositive() {
    for (String version : VALID_VERSIONS) {
      Ref ref = new Ref(version);

      if (canBeParsedAsLong(version)) {
        assertTrue(ref.isSingleVersion());
        assertEquals(Long.parseLong(version), ref.getVersion());
      }
      else
        assertThrows(NumberFormatException.class, () -> ref.getVersion());

      if (ALL_VERSIONS.equals(version)) {
        assertTrue(ref.isAllVersions());
        assertFalse(ref.isSingleVersion());
      }
      else {
        assertFalse(ref.isAllVersions());
        assertTrue(ref.isSingleVersion());
      }

      if (isNullOrEmpty(version) || HEAD_VERSION.equals(version))
        assertTrue(ref.isHead());
      else
        assertFalse(ref.isHead());
    }
  }

  @Test
  public void refPositive() {
    for (String refString : crossProduct(VALID_BRANCH_IDS, VALID_VERSIONS)) {
      Ref ref = refString.contains(":") || canBeParsedAsNonNegativeLong(refString) || ALL_VERSIONS.equals(refString)
          || HEAD_VERSION.equals(refString) ? new Ref(refString) : Ref.fromBranchId(refString);
      if (!refString.contains(":")) {
        if (isValidVersion(refString)) {
          //It should be one / multiple versions in the main branch
          assertEquals(MAIN_BRANCH, ref.getBranch());
          if (canBeParsedAsNonNegativeLong(refString)) {
            //It should be a dedicated version in the main branch
            assertTrue(ref.isSingleVersion());
            assertFalse(ref.isHead());
            assertFalse(ref.isAllVersions());
            assertEquals(Long.parseLong(refString), ref.getVersion());
          }
        }
        else {
          //It should be the HEAD version on a branch
          assertEquals(isNullOrEmpty(refString) ? MAIN_BRANCH : refString, ref.getBranch());
          assertTrue(ref.isHead());
          assertTrue(ref.isSingleVersion());
          assertFalse(ref.isAllVersions());
          assertThrows(NumberFormatException.class, () -> ref.getVersion());
        }
      }
    }
  }

  //TODO: Add negative tests
  //TODO: Add tag tests?
}
