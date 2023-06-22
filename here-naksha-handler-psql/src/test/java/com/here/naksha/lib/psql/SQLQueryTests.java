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

package com.here.naksha.lib.psql;

import static org.junit.jupiter.api.Assertions.*;

import com.here.naksha.handler.psql.SQLQueryExt;
import org.junit.jupiter.api.Test;

public class SQLQueryTests {

  @Test
  public void testVariableInheritance() {
    SQLQueryExt q = new SQLQueryExt("${{someFragment}} ${someVariable}");
    q.setVariable("someVariable", "someValue");
    q.setQueryFragment("someFragment", "${someVariable} ==");
    q.substitute();
    assertEquals("\"someValue\" == \"someValue\"", q.text());
  }

  @Test
  public void testParameterInheritance() {
    SQLQueryExt q = new SQLQueryExt("${{someFragment}} #{someParameter}");
    q.setNamedParameter("someParameter", "someValue");
    q.setQueryFragment("someFragment", "#{someParameter} ==");
    q.substitute();
    assertEquals("? == ?", q.text());
    assertEquals(2, q.parameters().size());
    assertEquals("someValue", q.parameters().get(0));
    assertEquals("someValue", q.parameters().get(1));
  }

  @Test
  public void testFragmentInheritance() {
    SQLQueryExt q = new SQLQueryExt("${{someInnerFragment}} ${{abc}}");
    q.setQueryFragment("abc", "someValue");
    q.setQueryFragment("someInnerFragment", new SQLQueryExt("${{abc}} =="));
    q.substitute();
    assertEquals("someValue == someValue", q.text());
  }
}
