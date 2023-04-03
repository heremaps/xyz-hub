package com.here.xyz.events;

import static com.here.xyz.events.QueryDelimiter.COLON;
import static com.here.xyz.events.QueryDelimiter.SEMICOLON;
import static org.junit.Assert.*;

import com.here.xyz.exceptions.XyzErrorException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

public class QueryParametersTest {

  @Test
  public void basic() throws XyzErrorException {
    final QueryParameters params = new QueryParameters("foo=bar&bar=1,'2'&foo=8&p%3Abee=p%3Ahello%26world");
    assertEquals(4, params.size());
    assertEquals(3, params.keySize());
    assertEquals(2, params.count("foo"));
    assertEquals(1, params.count("bar"));
    assertEquals(1, params.count("p:bee")); // The colon is URL encoded, therefore is not used as operator.
    assertEquals(0, params.count("x"));

    // Test assign

    final QueryParameter foo1 = params.get(0);
    assertNotNull(foo1);
    assertEquals(1, foo1.size());
    assertTrue(foo1.isString(0));
    assertEquals("bar", foo1.first());

    final QueryParameter bar = params.get(1);
    assertNotNull(bar);
    assertEquals(2, bar.size());
    assertTrue(bar.isLong(0));
    assertTrue(bar.isString(1));
    assertEquals(1L, bar.getLong(0, -1L));
    assertEquals("2", bar.getString(1));

    final QueryParameter foo2 = params.get(2);
    assertNotNull(foo2);
    assertEquals(1, foo2.size());
    assertTrue(foo2.isLong(0));
    assertEquals(8L, foo2.getLong(0, -1L));

    final QueryParameter pbee = params.get(3);
    assertNotNull(pbee);
    assertEquals("p:bee", pbee.getKey());
    assertEquals(1, pbee.size());
    assertTrue(pbee.isString(0));
    assertEquals("p:hello&world", pbee.getString(0));
  }

  // ----------------------------------------------------------------------------------------------------------------------------------
  // Test type overloading, force all values to be explicitly strings!
  //

  private static class QueryParamTest extends QueryParameters {

    public QueryParamTest(@Nullable CharSequence query) throws XyzErrorException {
      super(query);
    }

    @Override
    protected @NotNull QueryParameterType typeOfValue(@NotNull String key, int index) {
      return QueryParameterType.STRING;
    }

    @Override
    protected boolean stopOnDelimiter(final @Nullable String key, @Nullable QueryOperator op,
        final int index, final int number,
        final boolean quoted, final @NotNull QueryDelimiter delimiter, final @NotNull StringBuilder sb) {
      // Expand the colon.
      if (delimiter == COLON) {
        if (equals("p", sb)) {
          sb.append("roperties.");
          return false;
        }
        if (equals("properties.xyz", sb)) {
          sb.setLength("properties.".length());
          sb.append("@ns:com.here.xyz.");
          return false;
        }
      }
      // Allow semicolon as value separator.
      if (key != null && op != null && delimiter == SEMICOLON) {
        return true;
      }
      return super.stopOnDelimiter(key, op, index, number, quoted, delimiter, sb);
    }

    @Override
    protected boolean addDelimiter(@NotNull QueryParameter parameter, @NotNull QueryDelimiter delimiter) {
      if (delimiter == SEMICOLON && "properties.bee".equals(parameter.getKey())) {
        return true;
      }
      return super.addDelimiter(parameter, delimiter);
    }
  }

  @Test
  public void explicitStrings() throws XyzErrorException {
    final QueryParamTest params = new QueryParamTest("foo=bar&bar=1,'2'&foo=8&p:bee=p%3Ahello%26world&p:bee(gt)=p:xyz:hello%26world;x");
    assertEquals(5, params.size());
    assertEquals(3, params.keySize());
    assertEquals(2, params.count("foo"));
    assertEquals(1, params.count("bar"));
    assertEquals(2, params.count("properties.bee"));
    assertEquals(0, params.count("x"));

    final QueryParameter bar = params.get("bar");
    assertNotNull(bar);
    // Note: The QueryParamTest implementation forces all values to be strings!
    assertEquals("1", bar.get(0));
    assertEquals("2", bar.get(1));

    QueryParameter foo = params.get("foo");
    assertNotNull(foo);
    assertEquals("bar", foo.get(0));
    foo = foo.next();
    assertNotNull(foo);
    assertEquals("8", foo.get(0));
    foo = foo.next();
    assertNull(foo);

    // First &p:bee argument at index 3.
    QueryParameter pbee = params.get(3);
    assertNotNull(pbee);
    assertEquals(1, pbee.size());
    assertEquals("properties.bee", pbee.getKey());
    assertTrue(pbee.isString(0));
    // Because the colon is URL encoded (%3A), it should not have a semantic meaning and not trigger the replacement!
    assertEquals("p:hello&world", pbee.getString(0));

    // Second &p:bee argument at index 4.
    pbee = pbee.next();
    assertNotNull(pbee);
    assertSame(pbee, params.get(4));
    assertSame(QueryOperator.GREATER_THAN, pbee.getOp());
    assertEquals("properties.bee", pbee.getKey());

    // This should have three values
    assertEquals(3, pbee.size());
    assertTrue(pbee.isString(0));
    // Because this time the colon is not URL encoded it should have a semantic meaning and should be expanded!
    // First expand "p:" into "properties."
    // Then expand "properties.xyz:" into "properties.@ns:com.here.xyz.".
    assertEquals("properties.@ns:com.here.xyz.hello&world", pbee.getString(0));

    assertSame(SEMICOLON, pbee.get(1));
    assertEquals("x", pbee.get(2));
  }

  // ----------------------------------------------------------------------------------------------------------------------------------
}