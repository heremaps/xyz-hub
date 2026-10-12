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

package com.here.xyz.events;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;
import java.util.Arrays;
import java.util.List;

/**
 * Unit tests for PropertyQuery sanitization to prevent SQL injection vulnerabilities.
 * Tests cover the SQL injection scenarios via p.name.
 */
@DisplayName("PropertyQuery Sanitization Tests")
public class PropertyQueryTest {

  @Test
  @DisplayName("Scenario A: Valid SQL - x'||upper('a')||'y should be sanitized")
  void testSQLInjectionPayloadA_ValidSQL() {
    PropertyQuery pq = new PropertyQuery();
    pq.setKey("name");
    pq.setValues(Arrays.asList("x'||upper('a')||'y"));
    
    String value = (String) pq.getValues().get(0);
    assertNotNull(value, "Value should not be null after sanitization");
    assertFalse(value.contains("'"), "Single quotes should be stripped");
    assertFalse(value.contains("||"), "Pipe operators should be stripped");
  }

  @Test
  @DisplayName("Scenario D: Negative control - should be sanitized")
  void testSQLInjectionPayloadD_NegativeControl() {
    PropertyQuery pq = new PropertyQuery();
    pq.setKey("name");
    pq.setValues(Arrays.asList("x'||(CASE WHEN current_database()='xyZ' THEN 'a' ELSE '1' END)::int::text||'y"));
    
    String value = (String) pq.getValues().get(0);
    assertFalse(value.contains("'"), "Single quotes should be stripped");
    assertFalse(value.contains("||"), "Pipe operators should be stripped");
  }

  @Test
  @DisplayName("Legitimate: Plain alphanumeric string")
  void testLegitimateValue_PlainString() {
    PropertyQuery pq = new PropertyQuery();
    pq.setKey("name");
    pq.setValues(Arrays.asList("Main Street"));
    
    String value = (String) pq.getValues().get(0);
    assertEquals("Main Street", value, "Plain alphanumeric with space should pass");
  }

  @Test
  @DisplayName("Legitimate: String with dashes")
  void testLegitimateValue_WithDashes() {
    PropertyQuery pq = new PropertyQuery();
    pq.setKey("name");
    pq.setValues(Arrays.asList("O-Brien"));
    
    String value = (String) pq.getValues().get(0);
    assertEquals("O-Brien", value, "Dashes should be allowed");
  }

  @Test
  @DisplayName("Legitimate: Namespace with @ symbol")
  void testLegitimateValue_WithAtSymbol() {
    PropertyQuery pq = new PropertyQuery();
    pq.setKey("namespace");
    pq.setValues(Arrays.asList("@ns:com:here:xyz"));
    
    String value = (String) pq.getValues().get(0);
    assertEquals("@ns:com:here:xyz", value, "@ symbol should be allowed for namespaces");
  }

  @Test
  @DisplayName("Multiple values: mix of legitimate and malicious")
  void testMultipleValues_MixedLegitimateAndMalicious() {
    PropertyQuery pq = new PropertyQuery();
    pq.setKey("name");
    pq.setValues(Arrays.asList("Alice", "Bob", "x'||upper('c')||'y"));
    
    List<Object> values = pq.getValues();
    assertEquals(3, values.size(), "All values should be present");
    assertEquals("Alice", values.get(0), "First legitimate value");
    assertEquals("Bob", values.get(1), "Second legitimate value");
    
    String maliciousValue = (String) values.get(2);
    assertFalse(maliciousValue.contains("'"), "Malicious value should be sanitized");
    assertFalse(maliciousValue.contains("||"), "Malicious value should be sanitized");
  }

  @Test
  @DisplayName("Backslash operators should be stripped")
  void testBackslashOperatorsStripped() {
    PropertyQuery pq = new PropertyQuery();
    pq.setKey("name");
    pq.setValues(Arrays.asList("x'\\\\pg_backend_pid()\\\\'y"));
    
    String value = (String) pq.getValues().get(0);
    assertFalse(value.contains("\\"), "Backslashes should be stripped");
  }
}
