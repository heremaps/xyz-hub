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

package com.here.xyz.events;

import static com.here.xyz.events.PropertyQuery.QueryOperation.BEGINS_WITH;
import static com.here.xyz.events.PropertyQuery.QueryOperation.CONTAINS;
import static com.here.xyz.events.PropertyQuery.QueryOperation.EQUALS;
import static com.here.xyz.events.PropertyQuery.QueryOperation.GREATER_THAN_OR_EQUALS;
import static com.here.xyz.events.PropertyQuery.QueryOperation.LESS_THAN;
import static com.here.xyz.events.PropertyQuery.QueryOperation.NOT_EQUALS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.here.xyz.events.PropertyQuery.QueryOperation;
import com.jayway.jsonpath.JsonPath;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;

public class PropertiesQueryToJsonPathTest {

  private static PropertyQuery query(String key, QueryOperation operation, Object... values) {
    return new PropertyQuery()
        .withKey(key)
        .withOperation(operation)
        .withValues(Arrays.asList(values));
  }

  private static PropertiesQuery propertiesQuery(PropertyQuery... queries) {
    PropertyQueryList list = new PropertyQueryList();
    list.addAll(Arrays.asList(queries));
    PropertiesQuery pq = new PropertiesQuery();
    pq.add(list);
    return pq;
  }

  @Test
  public void emptyQueryReturnsNull() {
    assertNull(new PropertiesQuery().toJsonPath());
  }

  @Test
  public void emptyInnerListReturnsNull() {
    PropertiesQuery pq = new PropertiesQuery();
    pq.add(new PropertyQueryList());
    assertNull(pq.toJsonPath());
  }

  @Test
  public void singleStringEquals() {
    PropertiesQuery pq = propertiesQuery(query("properties.type", EQUALS, "building"));
    assertEquals("$[?(@.properties.type == 'building')]", pq.toJsonPath());
  }

  @Test
  public void numericValueIsNotQuoted() {
    PropertiesQuery pq = propertiesQuery(query("properties.count", EQUALS, 5L));
    assertEquals("$[?(@.properties.count == 5)]", pq.toJsonPath());
  }

  @Test
  public void booleanValueIsNotQuoted() {
    PropertiesQuery pq = propertiesQuery(query("properties.active", EQUALS, true));
    assertEquals("$[?(@.properties.active == true)]", pq.toJsonPath());
  }

  @Test
  public void nullValueRendersAsNullLiteral() {
    PropertiesQuery pq = propertiesQuery(query("properties.foo", EQUALS, (Object) null));
    assertEquals("$[?(@.properties.foo == null)]", pq.toJsonPath());
  }

  @Test
  public void multipleValuesAreCombinedWithOr() {
    PropertiesQuery pq = propertiesQuery(query("properties.type", EQUALS, "building", "road"));
    assertEquals("$[?((@.properties.type == 'building' || @.properties.type == 'road'))]", pq.toJsonPath());
  }

  @Test
  public void multipleQueriesInOneListAreCombinedWithAnd() {
    PropertiesQuery pq = propertiesQuery(
        query("properties.type", EQUALS, "building"),
        query("properties.height", GREATER_THAN_OR_EQUALS, 10L)
    );
    assertEquals("$[?(@.properties.type == 'building' && @.properties.height >= 10)]", pq.toJsonPath());
  }

  @Test
  public void multipleListsAreCombinedWithOr() {
    PropertyQueryList first = new PropertyQueryList();
    first.add(query("properties.type", EQUALS, "building"));
    PropertyQueryList second = new PropertyQueryList();
    second.add(query("properties.type", EQUALS, "road"));

    PropertiesQuery pq = new PropertiesQuery();
    pq.add(first);
    pq.add(second);

    assertEquals("$[?((@.properties.type == 'building') || (@.properties.type == 'road'))]", pq.toJsonPath());
  }

  @Test
  public void operatorMapping() {
    assertEquals("$[?(@.properties.a != 'x')]",
        propertiesQuery(query("properties.a", NOT_EQUALS, "x")).toJsonPath());
    assertEquals("$[?(@.properties.a < 3)]",
        propertiesQuery(query("properties.a", LESS_THAN, 3L)).toJsonPath());
  }

  @Test
  public void containsMapsToContainsKeyword() {
    PropertiesQuery pq = propertiesQuery(query("properties.tags", CONTAINS, "foo"));
    assertEquals("$[?(@.properties.tags contains 'foo')]", pq.toJsonPath());
  }

  @Test
  public void containsMatchesElementInArrayWhenEvaluatedByJayway() {
    PropertiesQuery pq = propertiesQuery(query("properties.tags", CONTAINS, "foo"));
    String jsonPath = pq.toJsonPath();

    //Feature whose "tags" array contains "foo" must be matched...
    String matchingDoc = "[{\"properties\":{\"tags\":[\"foo\",\"bar\"]}}]";
    List<Object> matched = JsonPath.read(matchingDoc, jsonPath);
    assertEquals(1, matched.size());

    //...while a feature whose "tags" array does not contain "foo" must not be matched.
    String nonMatchingDoc = "[{\"properties\":{\"tags\":[\"bar\",\"baz\"]}}]";
    List<Object> notMatched = JsonPath.read(nonMatchingDoc, jsonPath);
    assertTrue(notMatched.isEmpty());
  }

  @SuppressWarnings("deprecation")
  @Test
  public void beginsWithMapsToRegex() {
    PropertiesQuery pq = propertiesQuery(query("properties.name", BEGINS_WITH, "Main"));
    assertEquals("$[?(@.properties.name =~ /^Main.*/)]", pq.toJsonPath());
  }

  @Test
  public void stringValueWithSingleQuoteIsEscaped() {
    PropertiesQuery pq = propertiesQuery(query("properties.name", EQUALS, "O'Brien"));
    assertEquals("$[?(@.properties.name == 'O\\'Brien')]", pq.toJsonPath());
  }

  @Test
  public void fromStringIsConverted() {
    PropertiesQuery pq = PropertiesQuery.fromString("p.type=building&p.height>=10");
    assertEquals("$[?(@.properties.type == 'building' && @.properties.height >= 10)]", pq.toJsonPath());
  }

  @Test
  public void fromStringWithMultipleValues() {
    PropertiesQuery pq = PropertiesQuery.fromString("p.type=building,road");
    assertEquals("$[?((@.properties.type == 'building' || @.properties.type == 'road'))]", pq.toJsonPath());
  }
}




