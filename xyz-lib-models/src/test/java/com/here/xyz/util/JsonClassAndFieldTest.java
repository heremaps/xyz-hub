package com.here.xyz.util;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.lang.reflect.Field;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unused")
class JsonClassAndFieldTest {

  static class TestType {

    public boolean booleanValue;
    @JsonProperty("c")
    private char charValue;
    protected byte byteValue;
    @JsonProperty(defaultValue = "100")
    protected short shortValue;
    protected int intValue;
    protected long longValue;
    protected float floatValue;
    protected double doubleValue;
    @JsonProperty(defaultValue = "Hello")
    protected String stringValue;
    @JsonProperty(defaultValue = "Foo")
    protected Object objectValue;

    // -- hidden from json

    @JsonIgnore
    public String ignoreMe;
    private String pp;
  }

  @Test
  void basics() {
    final TestType test = new TestType();
    JsonClass<TestType> testClass = JsonClass.of(test);
    assertNotNull(testClass);
    assertEquals(10, testClass.fields.length);

    {
      final JsonField<TestType, Boolean> booleanField = testClass.getField(0, boolean.class);
      assertNotNull(booleanField);
      assertSame(Boolean.TRUE, booleanField.value(true));
      assertEquals(false, booleanField._get(test));
      assertSame(boolean.class, booleanField.valueClass);
      assertEquals(false, booleanField._get(test));
      assertEquals(false, booleanField.set(test, true));
      assertTrue(test.booleanValue);
      assertEquals("booleanValue", booleanField.jsonName);
      assertTrue(booleanField._compareAndSwap(test, true, false));
      assertFalse(test.booleanValue);
    }
    {
      final JsonField<TestType, Character> charField = testClass.getField(1, char.class);
      assertNotNull(charField);
      assertSame((char) 0, charField.value((char) 0));
      assertEquals((char) 0, charField._get(test));
      assertSame(char.class, charField.valueClass);
      assertEquals((char) 0, charField._get(test));
      assertEquals((char) 0, charField.set(test, (char) 1));
      assertEquals((char) 1, test.charValue);
      assertEquals("c", charField.jsonName);
      assertTrue(charField._compareAndSwap(test, (char) 1, (char) 2));
      assertEquals((char) 2, test.charValue);
    }

    // Numbers
    {
      final JsonField<TestType, Byte> byteField = testClass.getField(2, byte.class);
      assertNotNull(byteField);
      assertSame((byte) 0, byteField.value((byte) 0));
      assertSame((byte) 0, byteField.value(0L));
      assertEquals((byte) 0, byteField._get(test));
      assertSame(byte.class, byteField.valueClass);
      assertEquals((byte) 0, byteField._get(test));
      assertEquals((byte) 0, byteField.set(test, (byte) 1));
      assertEquals((byte) 1, test.byteValue);
      assertEquals("byteValue", byteField.jsonName);
      assertTrue(byteField._compareAndSwap(test, (byte) 1, (byte) 2));
      assertEquals((byte) 2, test.byteValue);
    }
    {
      final JsonField<TestType, Short> shortField = testClass.getField(3, short.class);
      assertNotNull(shortField);
      assertSame((short) 0, shortField.value((short) 0));
      assertSame((short) 0, shortField.value(0L));
      assertEquals((short) 0, shortField._get(test));
      assertSame(short.class, shortField.valueClass);
      assertEquals((short) 0, shortField._get(test));
      assertEquals((short) 0, shortField.set(test, (short) 1));
      assertEquals((short) 1, test.shortValue);
      assertEquals("shortValue", shortField.jsonName);
      assertTrue(shortField._compareAndSwap(test, (short) 1, (short) 2));
      assertEquals((short) 2, test.shortValue);
      assertEquals((short) 100, shortField.defaultValue());
    }
    {
      final JsonField<TestType, Integer> intField = testClass.getField(4, int.class);
      assertNotNull(intField);
      assertSame(0, intField.value(0));
      assertSame(0, intField.value(0L));
      assertSame(0, intField.value(0));
      assertEquals(0, intField._get(test));
      assertSame(int.class, intField.valueClass);
      assertEquals(0, intField._get(test));
      assertEquals(0, intField.set(test, 1));
      assertEquals(1, test.intValue);
      assertEquals("intValue", intField.jsonName);
      assertTrue(intField._compareAndSwap(test, 1, 2));
      assertEquals(2, test.intValue);
    }
    {
      final JsonField<TestType, Long> longField = testClass.getField(5, long.class);
      assertNotNull(longField);
      assertSame(0L, longField.value(0));
      assertSame(0L, longField.value((byte) 0));
      assertSame(0L, longField.value(0L));
      assertEquals(0L, longField._get(test));
      assertSame(long.class, longField.valueClass);
      assertEquals(0L, longField._get(test));
      assertEquals(0L, longField.set(test, 1L));
      assertEquals(1L, test.longValue);
      assertEquals("longValue", longField.jsonName);
      assertTrue(longField._compareAndSwap(test, 1L, 2L));
      assertEquals(2L, test.longValue);
    }
    {
      final JsonField<TestType, Float> floatField = testClass.getField(6, float.class);
      assertNotNull(floatField);
      assertEquals(0f, floatField.value(0f));
      assertEquals(0f, floatField.value(0L));
      assertEquals(0f, floatField._get(test));
      assertSame(float.class, floatField.valueClass);
      assertEquals(0f, floatField._get(test));
      assertEquals(0f, floatField.set(test, 1f));
      assertEquals(1f, test.floatValue);
      assertEquals("floatValue", floatField.jsonName);
      assertTrue(floatField._compareAndSwap(test, 1f, 2f));
      assertEquals(2f, test.floatValue);
    }
    {
      final JsonField<TestType, Double> doubleField = testClass.getField(7, double.class);
      assertNotNull(doubleField);
      assertEquals(0d, doubleField.value(0f));
      assertEquals(0d, doubleField.value(0L));
      assertEquals(0d, doubleField._get(test));
      assertSame(double.class, doubleField.valueClass);
      assertEquals(0d, doubleField._get(test));
      assertEquals(0d, doubleField.set(test, 1d));
      assertEquals(1d, test.doubleValue);
      assertEquals("doubleValue", doubleField.jsonName);
      assertTrue(doubleField._compareAndSwap(test, 1d, 2d));
      assertEquals(2d, test.doubleValue);
    }
    {
      final JsonField<TestType, String> stringField = testClass.getField(8, String.class);
      assertNotNull(stringField);
      assertNull(stringField.value(null));
      assertNull(stringField._get(test));
      assertSame(String.class, stringField.valueClass);
      assertNull(stringField._get(test));
      assertNull(stringField.set(test, "1"));
      assertEquals("1", test.stringValue);
      assertEquals("stringValue", stringField.jsonName);
      assertTrue(stringField._compareAndSwap(test, "1", "2"));
      assertEquals("2", test.stringValue);
      assertEquals("Hello", stringField.defaultValue());
    }
    {
      final JsonField<TestType, Object> objectField = testClass.getField(9, Object.class);
      assertNotNull(objectField);
      assertNull(objectField.value(null));
      assertNull(objectField._get(test));
      assertSame(Object.class, objectField.valueClass);
      assertNull(objectField._get(test));
      assertNull(objectField.set(test, "1"));
      assertEquals("1", test.objectValue);
      assertEquals("objectValue", objectField.jsonName);
      assertTrue(objectField._compareAndSwap(test, "1", "2"));
      assertEquals("2", test.objectValue);
      assertEquals("Foo", objectField.defaultValue());
    }
  }

  static class IllegalTestType {

    @JsonProperty(defaultValue = "X")
    public StringBuilder foo;
  }

  @Test
  void test_illegalType() {
    assertThrows(InternalError.class, () -> JsonClass.of(new IllegalTestType()));
  }

  @SuppressWarnings("ClassCanBeRecord")
  static final class OwnType {

    OwnType(@NotNull String value) {
      this.value = value;
    }

    public final String value;

    public @NotNull String toString() {
      return value;
    }
  }

  static class SomeStruct {

    public OwnType ownType;
  }

  static class TestField<OBJECT> extends JsonFieldObject<OBJECT, OwnType> {

    TestField(@NotNull JsonClass<OBJECT> jsonClass, @NotNull Field javaField, int index, @NotNull String jsonName,
        @Nullable String defaultValue) {
      super(jsonClass, javaField, index, jsonName, defaultValue);
    }
  }

  @Test
  void test_ownType() {
    final SomeStruct test = new SomeStruct();
    final JsonClass<SomeStruct> useOwnTypeJsonClass = JsonClass.of(test);
    final JsonField<SomeStruct, OwnType> ownField = useOwnTypeJsonClass.getField("ownType", OwnType.class);
    assertNotNull(ownField);
    assertSame(OwnType.class, ownField.valueClass);
    final OwnType old = ownField.set(test, new OwnType("Hello"));
    assertNull(old);
    assertEquals("Hello", ownField.get(test).toString());
  }

  @BeforeAll
  static void init() {
    assertNull(JsonField.constructors.get(OwnType.class));
    JsonField.register(OwnType.class, TestField::new);
    assertNotNull(JsonField.constructors.get(OwnType.class));
  }

  @AfterAll
  static void done() {
    assertNotNull(JsonField.constructors.get(OwnType.class));
    JsonField.constructors.remove(OwnType.class);
    assertNull(JsonField.constructors.get(OwnType.class));
  }
}