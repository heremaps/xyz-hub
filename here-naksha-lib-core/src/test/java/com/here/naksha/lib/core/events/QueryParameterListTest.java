/*
 * Copyright (C) 2017-2023 HERE Europe B.V.
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
package com.here.naksha.lib.core.events;

import static com.here.naksha.lib.core.models.payload.events.QueryDelimiter.AMPERSAND;
import static com.here.naksha.lib.core.models.payload.events.QueryDelimiter.COLON;
import static com.here.naksha.lib.core.models.payload.events.QueryDelimiter.END;
import static com.here.naksha.lib.core.models.payload.events.QueryDelimiter.EXCLAMATION_MARK;
import static com.here.naksha.lib.core.models.payload.events.QueryDelimiter.PLUS;
import static com.here.naksha.lib.core.models.payload.events.QueryParameterType.STRING;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.here.naksha.lib.core.exceptions.ParameterError;
import com.here.naksha.lib.core.models.payload.events.QueryDelimiter;
import com.here.naksha.lib.core.models.payload.events.QueryOperation;
import com.here.naksha.lib.core.models.payload.events.QueryParameter;
import com.here.naksha.lib.core.models.payload.events.QueryParameterDecoder;
import com.here.naksha.lib.core.models.payload.events.QueryParameterList;
import java.util.List;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

public class QueryParameterListTest {

  @Test
  public void paramWithNumericValue() throws Exception {
    final QueryParameterList params = new QueryParameterList("id=100");
    assertEquals(1, params.size());
    assertEquals(1, params.keySize());
    assertEquals(1, params.count("id"));

    String id = params.getValueAsString("id");
    assertNotNull(id);
    assertEquals(id, "100");
  }

  @Test
  public void tagsWithEncodedValues() throws Exception {
    final QueryParameterList params = new QueryParameterList("tags=one" + "&tags=two,three"
        + "&tags=four%2Cfive"
        + "&tags=six+seven"
        + "&tags=eight%2Bnine%40Ten"
        + "&tags=eleven");
    assertEquals(6, params.size());
    assertEquals(1, params.keySize());
    assertEquals(6, params.count("tags"));

    List<String> tagList = params.collectAllOfAsString("tags");
    int i = 0;
    assertEquals("one", tagList.get(i++));
    assertEquals("two", tagList.get(i++));
    assertEquals("three", tagList.get(i++));
    assertEquals("four,five", tagList.get(i++));
    assertEquals("six", tagList.get(i++));
    assertEquals("seven", tagList.get(i++));
    assertEquals("eight+nine@Ten", tagList.get(i++));
    assertEquals("eleven", tagList.get(i++));
  }

  @Test
  public void basic() throws Exception {
    final QueryParameterList params =
        new QueryParameterList("foo=bar&bar=1,'2'&foo=8&p%3Abee=p%3Ahello%26world&west=-1.94377758&north=45");
    assertEquals(6, params.size());
    assertEquals(5, params.keySize());
    assertEquals(2, params.count("foo"));
    assertEquals(1, params.count("bar"));
    assertEquals(1, params.count("p:bee"));
    assertEquals(1, params.count("west"));
    assertEquals(1, params.count("north"));
    assertEquals(0, params.count("x"));

    // Test assign

    final QueryParameter foo1 = params.get(0);
    assertNotNull(foo1);
    assertEquals(1, foo1.values().size());
    assertFalse(foo1.values().isDouble(0));
    assertTrue(foo1.values().isString(0));
    assertEquals("bar", foo1.values().first());

    final QueryParameter bar = params.get(1);
    assertNotNull(bar);
    assertEquals(2, bar.values().size());
    assertTrue(bar.values().isLong(0));
    assertTrue(bar.values().isString(1));
    assertEquals(1L, bar.values().getLong(0, -1L));
    assertEquals("2", bar.values().getString(1));

    final QueryParameter foo2 = params.get(2);
    assertNotNull(foo2);
    assertEquals(1, foo2.values().size());
    assertTrue(foo2.values().isLong(0));
    assertEquals(8L, foo2.values().getLong(0, -1L));

    final QueryParameter pbee = params.get(3);
    assertNotNull(pbee);
    assertEquals("p:bee", pbee.key());
    assertEquals(1, pbee.values().size());
    assertTrue(pbee.values().isString(0));
    assertEquals("p:hello&world", pbee.values().getString(0));

    final QueryParameter west = params.get(4);
    assertNotNull(west);
    assertEquals("west", west.key());
    assertEquals(1, west.values().size());
    assertTrue(west.values().isDouble(0));
    assertFalse(west.values().isString(0));
    assertEquals(-1.94377758, west.values().getDouble(0));

    final QueryParameter north = params.get(5);
    assertNotNull(north);
    assertEquals("north", north.key());
    assertEquals(1, north.values().size());
    assertTrue(north.values().isLong(0));
    assertFalse(north.values().isString(0));
    assertEquals(45.0, north.values().getLong(0).doubleValue());
  }

  // ----------------------------------------------------------------------------------------------------------------------------------
  // Test own custom decoder, force all values to be explicitly strings and expand some keys/values.
  //
  private static class CustomStringsOnlyDecoder extends QueryParameterDecoder {

    @Override
    protected boolean stopOnDelimiter(@NotNull QueryDelimiter delimiter, boolean wasUrlEncoded)
        throws ParameterError {
      if (!wasUrlEncoded) {
        if (parsingKey() || parsingValue()) {
          if (delimiter == COLON) {
            if (equals("p", sb)) { // "p:" -> "properties."
              sb.append("roperties.");
              return false;
            }
            if (equals("properties.xyz", sb)) { // "properties.xyz:" -> "properties.@ns:com.here.xyz."
              sb.setLength("properties.".length());
              sb.append("@ns:com.here.xyz.");
              return false;
            }
          }
        }
      }
      return super.stopOnDelimiter(delimiter, wasUrlEncoded);
    }

    @Override
    protected void addValueAndDelimiter(@NotNull QueryDelimiter postfix) throws ParameterError {
      assert parameter != null;
      final Object value = sbToValue(STRING);
      parameter.values().add(value);
      parameter.valuesDelimiter().add(postfix);
    }
  }

  @Test
  public void explicitStrings() throws Exception {
    final CustomStringsOnlyDecoder decoder = new CustomStringsOnlyDecoder();
    final QueryParameterList params =
        decoder.parse("foo=bar&bar=1,'2'&foo=8&p:bee=p%3Ahello%26world&p:bee;gt=p:xyz:hello%26world;x");

    assertEquals(5, params.size());
    assertEquals(3, params.keySize());
    assertEquals(2, params.count("foo"));
    assertEquals(1, params.count("bar"));
    assertEquals(2, params.count("properties.bee"));
    assertEquals(0, params.count("x"));

    final QueryParameter bar = params.get("bar");
    assertNotNull(bar);
    // Note: The QueryParamTest implementation forces all values to be strings!
    assertEquals("1", bar.values().get(0));
    assertEquals("2", bar.values().get(1));

    QueryParameter foo = params.get("foo");
    assertNotNull(foo);
    assertEquals("bar", foo.values().get(0));
    foo = foo.next();
    assertNotNull(foo);
    assertEquals("8", foo.values().get(0));
    foo = foo.next();
    assertNull(foo);

    // First &p:bee argument at index 3.
    QueryParameter pbee = params.get(3);
    assertNotNull(pbee);
    assertEquals(1, pbee.values().size());
    assertEquals("properties.bee", pbee.key());
    assertTrue(pbee.values().isString(0));
    // Because the colon is URL encoded (%3A), it should not have a semantic meaning and not trigger
    // the replacement!
    assertEquals("p:hello&world", pbee.values().getString(0));

    // Second &p:bee argument at index 4.
    pbee = pbee.next();
    assertNotNull(pbee);
    assertSame(pbee, params.get(4));
    assertEquals("gt", pbee.arguments().first());
    assertEquals("properties.bee", pbee.key());

    // This should have three values
    assertEquals(1, pbee.arguments().size());
    assertEquals("gt", pbee.arguments().first());
    assertEquals(2, pbee.values().size());
    assertTrue(pbee.values().isString(0));
    assertTrue(pbee.values().isString(1));
    // Because this time the colon is not URL encoded it should have a semantic meaning and should
    // be expanded!
    // First expand "p:" into "properties."
    // Then expand "properties.xyz:" into "properties.@ns:com.here.xyz.".
    assertEquals("properties.@ns:com.here.xyz.hello&world", pbee.values().getString(0));
    assertEquals("x", pbee.values().get(1));
  }

  // ----------------------------------------------------------------------------------------------------------------------------------
  private static class CustomOpDecoder extends QueryParameterDecoder {

    @Override
    protected boolean stopOnDelimiter(@NotNull QueryDelimiter delimiter, boolean wasUrlEncoded)
        throws ParameterError {
      if (!wasUrlEncoded && (delimiter == PLUS || delimiter == EXCLAMATION_MARK)) {
        return true;
      }
      return super.stopOnDelimiter(delimiter, wasUrlEncoded);
    }
  }

  @Test
  public void testOperations() throws Exception {
    final QueryParameterList params = new QueryParameterList("foo=gt=5+4&bar='5'&x!=5", new CustomOpDecoder());
    assertEquals(3, params.size());
    assertEquals(3, params.keySize());
    assertEquals(1, params.count("foo"));
    assertEquals(1, params.count("bar"));
    assertEquals(1, params.count("x"));
    {
      QueryParameter foo = params.get("foo");
      assertNotNull(foo);
      assertFalse(foo.hasArguments());
      assertTrue(foo.hasValues());
      assertEquals(2, foo.values().size());
      assertSame(QueryOperation.GREATER_THAN, foo.op());
      assertEquals((Long) 5L, foo.values().getLong(0));
      assertSame(PLUS, foo.valuesDelimiter().get(0));
      assertEquals((Long) 4L, foo.values().getLong(1));
      assertSame(AMPERSAND, foo.valuesDelimiter().get(1));
    }
    {
      QueryParameter bar = params.get("bar");
      assertNotNull(bar);
      assertFalse(bar.hasArguments());
      assertTrue(bar.hasValues());
      assertEquals(1, bar.values().size());
      assertEquals("5", bar.values().getString(0));
      assertSame(AMPERSAND, bar.valuesDelimiter().get(0));
    }
    {
      QueryParameter x = params.get("x");
      assertNotNull(x);
      assertFalse(x.hasArguments());
      assertTrue(x.hasValues());
      assertSame(QueryOperation.NOT_EQUALS, x.op());

      assertEquals(1, x.values().size());
      assertEquals((Long) 5L, x.values().getLong(0));
      assertSame(END, x.valuesDelimiter().get(0));
    }
  }
}
