package com.here.xyz;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
class JsonObjectTest {

  @Test
  void test_empty() {
    final JsonObject empty = new JsonObject();
    assertEquals(0, empty.size());
  }

  private static class TestType extends JsonObject {

    @JsonProperty("foo")
    int bar;
  }

  @Test
  void test_props() {
    final TestType p = new TestType();
    assertEquals(1, p.size());
    assertEquals(0, (int) p.get("foo"));
    p.bar = 5;
    assertEquals(5, (int) p.get("foo"));
    p.put("bar", "Hello");
    assertEquals(2, p.size());
    p.clear();
    assertEquals(1, p.size());
    assertEquals(0, (int) p.get("foo"));
    assertEquals(0, p.bar);
    assertEquals(p.bar, (int) p.get("foo"));
  }
}