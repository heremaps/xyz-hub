package com.here.xyz;

import static org.junit.Assert.*;

import org.junit.Test;

public class StringHelperTest {
  @Test
  public void equals_test() {
    assertTrue(StringHelper.equals("a", "a"));
    assertTrue(StringHelper.equals("Hello", "Hello"));
    assertFalse(StringHelper.equals("Hello", "Hello World"));
    assertTrue(StringHelper.equals("Hello", 0, 5, "Hello World", 0, 5));
    assertTrue(StringHelper.equals("Hello", 1, 4, "Hello World", 1, 4));
    assertTrue(StringHelper.equals("Hello", 0, 0, "Hello World", 0, 0));
    assertFalse(StringHelper.equals("Hello", 0, 5, "Hello World", 1, 6));
    assertTrue(StringHelper.equals("ello ", 0, 5, "Hello World", 1, 6));
  }
}