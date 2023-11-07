/*
 * Copyright (C) 2017-2022 HERE Europe B.V.
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

package com.here.xyz.psql;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SQLQueryTests {

  @Test
  public void testVariableInheritance() {
    SQLQuery q = new SQLQuery("${{someFragment}} ${someVariable}")
        .withVariable("someVariable", "someValue")
        .withQueryFragment("someFragment", "${someVariable} ==");
    assertEquals("\"someValue\" == \"someValue\"", q.substitute().text());
  }

  @Test
  public void testParameterInheritance() {
    SQLQuery q = new SQLQuery("${{someFragment}} #{someParameter}")
        .withNamedParameter("someParameter", "someValue")
        .withQueryFragment("someFragment", "#{someParameter} ==");
    q.substitute();
    assertEquals("? == ?", q.text());
    assertEquals(2, q.parameters().size());
    assertEquals("someValue", q.parameters().get(0));
    assertEquals("someValue", q.parameters().get(1));
  }

  @Test
  public void testFragmentInheritance() {
    SQLQuery q = new SQLQuery("${{someInnerFragment}} ${{abc}}")
        .withQueryFragment("abc", "someValue")
        .withQueryFragment("someInnerFragment", new SQLQuery("${{abc}} =="));
    assertEquals("someValue == someValue", q.substitute().text());
  }

  @Test
  public void testConflictingQueryFragmentNames() {
    SQLQuery q = new SQLQuery("${{fragmentA}} ${{fragmentB}}")
        .withQueryFragment("fragmentA", new SQLQuery("${{frag}}").withQueryFragment("frag", "Hello"))
        .withQueryFragment("fragmentB", new SQLQuery("${{frag}}").withQueryFragment("frag", "World"));
    assertEquals("Hello World", q.substitute().text());
  }

}
